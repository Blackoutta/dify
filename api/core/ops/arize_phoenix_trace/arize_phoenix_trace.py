import hashlib
import json
import logging
import os
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Optional, Union, cast

from openinference.semconv.trace import OpenInferenceMimeTypeValues, OpenInferenceSpanKindValues, SpanAttributes
from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter as GrpcOTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter as HttpOTLPSpanExporter
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.id_generator import RandomIdGenerator
from opentelemetry.trace import SpanContext, Status, StatusCode, TraceFlags, TraceState, get_current_span, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from core.ops.base_trace_instance import BaseTraceInstance
from core.ops.entities.config_entity import ArizeConfig, PhoenixConfig
from core.ops.entities.trace_entity import (
    BaseTraceInfo,
    DatasetRetrievalTraceInfo,
    GenerateNameTraceInfo,
    MessageTraceInfo,
    ModerationTraceInfo,
    SuggestedQuestionTraceInfo,
    ToolTraceInfo,
    TraceTaskName,
    WorkflowTraceInfo,
)
from core.ops.exceptions import PendingTraceParentContextError
from core.ops.trace_context import parent_trace_context_from_metadata
from extensions.ext_database import db
from extensions.ext_redis import redis_client
from models.model import App, EndUser, MessageFile
from models.workflow import WorkflowNodeExecutionModel, WorkflowRun

logger = logging.getLogger(__name__)

_PHOENIX_PARENT_SPAN_CONTEXT_TTL_SECONDS = 300
_TRACEPARENT_PATTERN = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace_id>[0-9a-f]{32})-(?P<span_id>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})$"
)


def _build_parent_span_bridge_id(parent_workflow_run_id: str, node_id: str) -> str:
    return f"{parent_workflow_run_id}:{node_id}"


def _phoenix_parent_span_redis_key(parent_node_execution_id: str) -> str:
    return f"trace:phoenix:parent_span:{parent_node_execution_id}"


def _resolve_session_id(
    *,
    trace_session_id: str | None,
    conversation_id: str | None,
    workflow_run_id: str | None,
    parent_workflow_run_id: str | None,
) -> str:
    return trace_session_id or conversation_id or parent_workflow_run_id or workflow_run_id or ""


def _trace_session_id_from_metadata(metadata: Mapping[str, Any]) -> str | None:
    trace_session_id = metadata.get("trace_session_id")
    return trace_session_id if isinstance(trace_session_id, str) and trace_session_id else None


def _resolve_message_session_id(*, metadata: Mapping[str, Any], conversation_id: str | None) -> str | None:
    return _trace_session_id_from_metadata(metadata) or conversation_id


def _sanitize_span_name_part(value: str | None, *, limit: int | None = None) -> str:
    if not value:
        return ""
    sanitized = value.replace(" ", "_").replace("-", "_")
    return sanitized[:limit] if limit else sanitized


def _extract_json_mapping(raw_value: Any) -> dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if not raw_value:
        return {}
    if isinstance(raw_value, str):
        try:
            parsed = json.loads(raw_value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _build_workflow_span_name(trace_info: WorkflowTraceInfo, parent_workflow_run_id: str | None) -> str:
    app_name = str(trace_info.metadata.get("app_name") or "workflow")
    app_name_clean = _sanitize_span_name_part(app_name, limit=20) or "workflow"
    workflow_id_short = (trace_info.workflow_run_id or "unknown")[:8]
    prefix = "nested_" if parent_workflow_run_id else ""
    return f"{prefix}{app_name_clean}_{workflow_id_short}"


def _extract_tool_name(node_execution: Any, node_metadata: Mapping[str, Any]) -> str:
    tool_info = node_metadata.get("tool_info")
    if isinstance(tool_info, Mapping):
        for key in ("tool_name", "name", "provider_name"):
            value = tool_info.get(key)
            if isinstance(value, str) and value:
                return value

    process_data = _extract_json_mapping(getattr(node_execution, "process_data", None))
    for key in ("tool_name", "provider_name"):
        value = process_data.get(key)
        if isinstance(value, str) and value:
            return value

    return ""


def _build_node_span_name(
    node_execution: Any,
    process_data: Mapping[str, Any],
    node_metadata: Mapping[str, Any],
) -> str:
    node_type = str(getattr(node_execution, "node_type", "") or "node")
    title = str(getattr(node_execution, "title", "") or "")
    title_clean = _sanitize_span_name_part(title, limit=20)

    if title_clean and title_clean.lower() != node_type.lower():
        base_name = f"{node_type}_{title_clean}"
    elif title_clean:
        base_name = node_type
    else:
        base_name = f"{node_type}_{str(getattr(node_execution, 'id', 'unknown'))[:8]}"

    if node_type == "llm":
        model_name = _sanitize_span_name_part(str(process_data.get("model_name") or ""), limit=24)
        return f"{base_name}_{model_name}" if model_name else base_name

    if node_type == "tool":
        tool_name = _extract_tool_name(node_execution, node_metadata)
        return f"{base_name}_[{tool_name[:20]}]" if tool_name else f"{base_name}_tool"

    if node_type == "question-classifier":
        return f"{base_name}_classifier"
    if node_type == "if-else":
        return f"{base_name}_condition"
    if node_type == "loop":
        return f"{base_name}_main_loop"

    return base_name


def _attribute_value(value: Any) -> str | int | float | bool:
    if isinstance(value, str | int | float | bool):
        return value
    if value is None:
        return ""
    return json.dumps(value, ensure_ascii=False, default=str)


def _prefixed_attributes(prefix: str, values: Mapping[str, Any]) -> dict[str, str | int | float | bool]:
    return {
        f"{prefix}.{key}": _attribute_value(value)
        for key, value in values.items()
        if value is not None
    }


def _set_span_status(span: Any, error: Exception | str | None = None) -> None:
    if error:
        span.set_status(Status(StatusCode.ERROR, str(error)))
    else:
        span.set_status(Status(StatusCode.OK))


def _publish_parent_span_context(parent_node_execution_id: str, carrier: Mapping[str, str]) -> None:
    redis_client.setex(
        _phoenix_parent_span_redis_key(parent_node_execution_id),
        _PHOENIX_PARENT_SPAN_CONTEXT_TTL_SECONDS,
        json.dumps(dict(carrier), ensure_ascii=False),
    )


def _publish_parent_span_context_aliases(node_execution: Any, carrier: Mapping[str, str]) -> None:
    workflow_run_id = getattr(node_execution, "workflow_run_id", None)
    if not workflow_run_id:
        return

    identifiers = [
        getattr(node_execution, "node_execution_id", None),
        getattr(node_execution, "id", None),
        getattr(node_execution, "node_id", None),
    ]
    published_keys: set[str] = set()
    for identifier in identifiers:
        if not identifier:
            continue
        key = _build_parent_span_bridge_id(str(workflow_run_id), str(identifier))
        if key in published_keys:
            continue
        _publish_parent_span_context(key, carrier)
        published_keys.add(key)


def _remember_node_span_aliases(node_execution: Any, node_span: Any, node_spans_by_node_id: dict[str, Any]) -> None:
    for identifier in (
        getattr(node_execution, "node_id", None),
        getattr(node_execution, "node_execution_id", None),
        getattr(node_execution, "id", None),
    ):
        if identifier:
            node_spans_by_node_id[str(identifier)] = node_span


def _get_node_execution_id(node_execution: Any) -> str:
    return str(getattr(node_execution, "id", None) or getattr(node_execution, "node_execution_id", ""))


def _build_execution_id_by_node_id(node_executions: list[Any]) -> dict[str, str]:
    execution_id_by_node_id: dict[str, str] = {}
    ambiguous_node_ids: set[str] = set()

    for node_execution in node_executions:
        node_id = getattr(node_execution, "node_id", None)
        if not isinstance(node_id, str) or not node_id:
            continue

        execution_id = _get_node_execution_id(node_execution)
        if node_id in ambiguous_node_ids:
            continue

        existing_execution_id = execution_id_by_node_id.get(node_id)
        if existing_execution_id is None:
            execution_id_by_node_id[node_id] = execution_id
            continue

        if existing_execution_id != execution_id:
            ambiguous_node_ids.add(node_id)
            execution_id_by_node_id.pop(node_id, None)

    return execution_id_by_node_id


def _build_graph_parent_index(node_executions: list[Any]) -> dict[str, str]:
    execution_id_by_node_id = _build_execution_id_by_node_id(node_executions)
    graph_parent_index: dict[str, str] = {}

    for node_execution in node_executions:
        predecessor_node_id = getattr(node_execution, "predecessor_node_id", None)
        if not isinstance(predecessor_node_id, str):
            continue

        predecessor_execution_id = execution_id_by_node_id.get(predecessor_node_id)
        if predecessor_execution_id is not None:
            graph_parent_index[_get_node_execution_id(node_execution)] = predecessor_execution_id

    return graph_parent_index


def _resolve_structured_parent_execution_id(
    node_execution: Any,
    node_metadata: Mapping[str, Any],
    execution_id_by_node_id: Mapping[str, str],
) -> str | None:
    for container_key in ("iteration_id", "loop_id"):
        container_id = node_metadata.get(container_key) or getattr(node_execution, container_key, None)
        if not isinstance(container_id, str) or not container_id:
            continue

        container_execution_id = execution_id_by_node_id.get(container_id)
        if container_execution_id is not None:
            return container_execution_id

    return None


def _resolve_node_parent_span_by_execution(
    *,
    execution_id: str,
    structured_parent_execution_id: str | None,
    span_by_execution_id: Mapping[str, Any],
    graph_parent_index: Mapping[str, str],
    workflow_span: Any,
) -> Any:
    graph_parent_execution_id = graph_parent_index.get(execution_id)
    if graph_parent_execution_id is not None:
        graph_parent_span = span_by_execution_id.get(graph_parent_execution_id)
        if graph_parent_span is not None:
            return graph_parent_span

    if structured_parent_execution_id is not None:
        structured_parent_span = span_by_execution_id.get(structured_parent_execution_id)
        if structured_parent_span is not None:
            return structured_parent_span

    return workflow_span


def _resolve_node_parent_span(
    node_execution: Any,
    node_metadata: Mapping[str, Any],
    node_spans_by_node_id: Mapping[str, Any],
    workflow_span: Any,
) -> Any:
    predecessor_node_id = getattr(node_execution, "predecessor_node_id", None)
    if predecessor_node_id and predecessor_node_id in node_spans_by_node_id:
        return node_spans_by_node_id[str(predecessor_node_id)]

    node_id = str(getattr(node_execution, "node_id", "") or "")
    for container_key in ("loop_id", "iteration_id"):
        container_id = node_metadata.get(container_key)
        if isinstance(container_id, str) and container_id and container_id != node_id:
            container_span = node_spans_by_node_id.get(container_id)
            if container_span:
                return container_span

    return workflow_span


def _resolve_published_parent_span_context(parent_node_execution_id: str) -> dict[str, str]:
    raw_carrier = redis_client.get(_phoenix_parent_span_redis_key(parent_node_execution_id))
    if raw_carrier is None:
        raise PendingTraceParentContextError(parent_node_execution_id)
    if isinstance(raw_carrier, bytes):
        raw_carrier = raw_carrier.decode("utf-8")

    carrier = json.loads(raw_carrier)
    if not isinstance(carrier, dict):
        raise ValueError(f"Phoenix parent span context must be a JSON object: {parent_node_execution_id}")

    normalized_carrier = {str(key): str(value) for key, value in carrier.items()}
    traceparent = normalized_carrier.get("traceparent")
    if not traceparent or _TRACEPARENT_PATTERN.fullmatch(traceparent) is None:
        raise ValueError(f"Phoenix parent span context has invalid traceparent: {parent_node_execution_id}")

    extracted_context = TraceContextTextMapPropagator().extract(carrier=normalized_carrier)
    extracted_span_context = get_current_span(extracted_context).get_span_context()
    if not extracted_span_context.is_valid or not extracted_span_context.is_remote:
        raise ValueError(f"Phoenix parent span context could not be restored: {parent_node_execution_id}")

    return normalized_carrier


def _app_uses_phoenix_provider(app_tracing_config: Mapping[str, Any] | None) -> bool:
    if not app_tracing_config or not app_tracing_config.get("enabled"):
        return False
    return app_tracing_config.get("tracing_provider") in {"arize", "phoenix"}


def _parent_workflow_can_publish_span_context(parent_workflow_run_id: str) -> bool:
    parent_run = db.session.query(WorkflowRun).filter(WorkflowRun.id == parent_workflow_run_id).first()
    if parent_run is None:
        return True

    parent_app = db.session.query(App).filter(App.id == parent_run.app_id).first()
    if parent_app is None or not parent_app.tracing:
        return False

    try:
        app_tracing_config = json.loads(parent_app.tracing)
    except (TypeError, json.JSONDecodeError):
        return False
    if not isinstance(app_tracing_config, Mapping):
        return False

    return _app_uses_phoenix_provider(app_tracing_config)


def _resolve_workflow_parent_carrier(
    parent_node_execution_id: str,
    parent_workflow_run_id: str | None,
) -> dict[str, str] | None:
    try:
        return _resolve_published_parent_span_context(parent_node_execution_id)
    except PendingTraceParentContextError:
        if parent_workflow_run_id and not _parent_workflow_can_publish_span_context(parent_workflow_run_id):
            logger.info(
                "[Arize/Phoenix] Parent workflow %s cannot publish parent span context; using fallback root",
                parent_workflow_run_id,
            )
            return None
        raise


def setup_tracer(arize_phoenix_config: ArizeConfig | PhoenixConfig) -> tuple[trace_sdk.Tracer, SimpleSpanProcessor]:
    """Configure OpenTelemetry tracer with OTLP exporter for Arize/Phoenix."""
    try:
        # Choose the appropriate exporter based on config type
        exporter: Union[GrpcOTLPSpanExporter, HttpOTLPSpanExporter]
        if isinstance(arize_phoenix_config, ArizeConfig):
            arize_endpoint = f"{arize_phoenix_config.endpoint}/v1"
            arize_headers = {
                "api_key": arize_phoenix_config.api_key or "",
                "space_id": arize_phoenix_config.space_id or "",
                "authorization": f"Bearer {arize_phoenix_config.api_key or ''}",
            }
            exporter = GrpcOTLPSpanExporter(
                endpoint=arize_endpoint,
                headers=arize_headers,
                timeout=30,
            )
        else:
            phoenix_endpoint = f"{arize_phoenix_config.endpoint}/v1/traces"
            phoenix_headers = {
                "api_key": arize_phoenix_config.api_key or "",
                "authorization": f"Bearer {arize_phoenix_config.api_key or ''}",
            }
            exporter = HttpOTLPSpanExporter(
                endpoint=phoenix_endpoint,
                headers=phoenix_headers,
                timeout=30,
            )

        attributes = {
            "openinference.project.name": arize_phoenix_config.project or "",
            "model_id": arize_phoenix_config.project or "",
        }
        resource = Resource(attributes=attributes)
        provider = trace_sdk.TracerProvider(resource=resource)
        processor = SimpleSpanProcessor(
            exporter,
        )
        provider.add_span_processor(processor)

        # Create a named tracer instead of setting the global provider
        tracer_name = f"arize_phoenix_tracer_{arize_phoenix_config.project}"
        logger.info(f"[Arize/Phoenix] Created tracer with name: {tracer_name}")
        return cast(trace_sdk.Tracer, provider.get_tracer(tracer_name)), processor
    except Exception as e:
        logger.error(f"[Arize/Phoenix] Failed to setup the tracer: {str(e)}", exc_info=True)
        raise


def datetime_to_nanos(dt: Optional[datetime]) -> int:
    """Convert datetime to nanoseconds since epoch. If None, use current time."""
    if dt is None:
        dt = datetime.now()
    return int(dt.timestamp() * 1_000_000_000)


def uuid_to_trace_id(string: Optional[str]) -> int:
    """Convert UUID string to a valid trace ID (16-byte integer)."""
    if string is None:
        string = ""
    hash_object = hashlib.sha256(string.encode())

    # Take the first 16 bytes (128 bits) of the hash
    digest = hash_object.digest()[:16]

    # Convert to integer (128 bits)
    return int.from_bytes(digest, byteorder="big")


class ArizePhoenixDataTrace(BaseTraceInstance):
    def __init__(
        self,
        arize_phoenix_config: ArizeConfig | PhoenixConfig,
    ):
        super().__init__(arize_phoenix_config)
        import logging

        logging.basicConfig()
        logging.getLogger().setLevel(logging.DEBUG)
        self.arize_phoenix_config = arize_phoenix_config
        self.tracer, self.processor = setup_tracer(arize_phoenix_config)
        self.project = arize_phoenix_config.project
        self.file_base_url = os.getenv("FILES_URL", "http://127.0.0.1:5001")
        self.propagator = TraceContextTextMapPropagator()
        self.dify_trace_ids: set[str] = set()
        self.root_span_carriers: dict[str, dict[str, str]] = {}
        self.carrier: dict[str, str] = {}

    def trace(self, trace_info: BaseTraceInfo):
        logger.info(f"[Arize/Phoenix] Trace: {trace_info}")
        try:
            if isinstance(trace_info, WorkflowTraceInfo):
                self.workflow_trace(trace_info)
            if isinstance(trace_info, MessageTraceInfo):
                self.message_trace(trace_info)
            if isinstance(trace_info, ModerationTraceInfo):
                self.moderation_trace(trace_info)
            if isinstance(trace_info, SuggestedQuestionTraceInfo):
                self.suggested_question_trace(trace_info)
            if isinstance(trace_info, DatasetRetrievalTraceInfo):
                self.dataset_retrieval_trace(trace_info)
            if isinstance(trace_info, ToolTraceInfo):
                self.tool_trace(trace_info)
            if isinstance(trace_info, GenerateNameTraceInfo):
                self.generate_name_trace(trace_info)

        except Exception as e:
            logger.error(f"[Arize/Phoenix] Error in the trace: {str(e)}", exc_info=True)
            raise

    def workflow_trace(self, trace_info: WorkflowTraceInfo):
        workflow_metadata = {
            "workflow_run_id": trace_info.workflow_run_id or "",
            "message_id": trace_info.message_id or "",
            "workflow_app_log_id": trace_info.workflow_app_log_id or "",
            "status": trace_info.workflow_run_status or "",
            "status_message": trace_info.error or "",
            "level": "ERROR" if trace_info.error else "DEFAULT",
            "total_tokens": trace_info.total_tokens or 0,
        }
        workflow_metadata.update(trace_info.metadata)

        parent_context = parent_trace_context_from_metadata(trace_info.metadata)
        parent_workflow_run_id = parent_context.parent_workflow_run_id if parent_context else None
        parent_node_execution_id = parent_context.parent_node_execution_id if parent_context else None
        trace_session_id = _trace_session_id_from_metadata(trace_info.metadata)
        session_id = _resolve_session_id(
            trace_session_id=trace_session_id,
            conversation_id=trace_info.conversation_id,
            workflow_run_id=trace_info.workflow_run_id,
            parent_workflow_run_id=parent_workflow_run_id,
        )

        carrier = (
            _resolve_workflow_parent_carrier(parent_node_execution_id, parent_workflow_run_id)
            if parent_node_execution_id
            else None
        )
        if carrier is None:
            trace_id_source = parent_workflow_run_id or trace_info.workflow_run_id or trace_info.message_id
            carrier = self.ensure_root_span(
                trace_id_source,
                root_span_name=trace_info.workflow_run_id,
                start_time=datetime_to_nanos(trace_info.start_time),
                end_time=datetime_to_nanos(trace_info.end_time),
                root_span_attributes={
                    SpanAttributes.INPUT_VALUE: json.dumps(trace_info.workflow_run_inputs, ensure_ascii=False),
                    SpanAttributes.INPUT_MIME_TYPE: OpenInferenceMimeTypeValues.JSON.value,
                    SpanAttributes.OUTPUT_VALUE: json.dumps(trace_info.workflow_run_outputs, ensure_ascii=False),
                    SpanAttributes.OUTPUT_MIME_TYPE: OpenInferenceMimeTypeValues.JSON.value,
                    SpanAttributes.SESSION_ID: session_id,
                },
            )
        parent_otel_context = self.propagator.extract(carrier=carrier)

        workflow_span = self.tracer.start_span(
            name=_build_workflow_span_name(trace_info, parent_workflow_run_id),
            attributes={
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.workflow_run_inputs, ensure_ascii=False),
                SpanAttributes.INPUT_MIME_TYPE: OpenInferenceMimeTypeValues.JSON.value,
                SpanAttributes.OUTPUT_VALUE: json.dumps(trace_info.workflow_run_outputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_MIME_TYPE: OpenInferenceMimeTypeValues.JSON.value,
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
                SpanAttributes.METADATA: json.dumps(workflow_metadata, ensure_ascii=False),
                SpanAttributes.SESSION_ID: session_id,
                **_prefixed_attributes(
                    "dify.workflow",
                    {
                        "run_id": trace_info.workflow_run_id,
                        "id": trace_info.workflow_id,
                        "status": trace_info.workflow_run_status,
                        "app_id": trace_info.metadata.get("app_id"),
                        "app_name": trace_info.metadata.get("app_name"),
                        "workspace_name": trace_info.metadata.get("workspace_name"),
                    },
                ),
            },
            start_time=datetime_to_nanos(trace_info.start_time),
            context=parent_otel_context,
        )

        try:
            # Process workflow nodes
            workflow_nodes = list(self._get_workflow_nodes(trace_info.workflow_run_id))
            execution_id_by_node_id = _build_execution_id_by_node_id(workflow_nodes)
            graph_parent_index = _build_graph_parent_index(workflow_nodes)
            node_execution_by_execution_id = {
                _get_node_execution_id(node_execution): node_execution for node_execution in workflow_nodes
            }
            span_by_execution_id: dict[str, Any] = {}
            emitting_execution_ids: set[str] = set()

            def emit_node_span(node_execution: Any) -> Any:
                execution_id = _get_node_execution_id(node_execution)
                existing_span = span_by_execution_id.get(execution_id)
                if existing_span is not None:
                    return existing_span

                created_at = node_execution.created_at or datetime.now()
                elapsed_time = node_execution.elapsed_time or 0.0
                finished_at = created_at + timedelta(seconds=elapsed_time)

                process_data = _extract_json_mapping(node_execution.process_data)

                node_metadata = {
                    "node_id": node_execution.id,
                    "graph_node_id": node_execution.node_id,
                    "node_type": node_execution.node_type,
                    "node_status": node_execution.status,
                    "tenant_id": node_execution.tenant_id,
                    "app_id": node_execution.app_id,
                    "app_name": node_execution.title,
                    "status": node_execution.status,
                    "level": "ERROR" if node_execution.status != "succeeded" else "DEFAULT",
                }

                if node_execution.execution_metadata:
                    node_metadata.update(_extract_json_mapping(node_execution.execution_metadata))

                structured_parent_execution_id = _resolve_structured_parent_execution_id(
                    node_execution,
                    node_metadata,
                    execution_id_by_node_id,
                )
                if execution_id not in emitting_execution_ids:
                    emitting_execution_ids.add(execution_id)
                    try:
                        for parent_execution_id in (
                            graph_parent_index.get(execution_id),
                            structured_parent_execution_id,
                        ):
                            if parent_execution_id is None or parent_execution_id == execution_id:
                                continue
                            if parent_execution_id in span_by_execution_id:
                                continue
                            parent_node_execution = node_execution_by_execution_id.get(parent_execution_id)
                            if parent_node_execution is not None:
                                emit_node_span(parent_node_execution)
                    finally:
                        emitting_execution_ids.discard(execution_id)

                # Determine the correct span kind based on node type
                span_kind = OpenInferenceSpanKindValues.CHAIN.value
                if node_execution.node_type == "llm":
                    span_kind = OpenInferenceSpanKindValues.LLM.value
                    provider = process_data.get("model_provider")
                    model = process_data.get("model_name")
                    if provider:
                        node_metadata["ls_provider"] = provider
                    if model:
                        node_metadata["ls_model_name"] = model

                    outputs = _extract_json_mapping(node_execution.outputs).get("usage", {})
                    usage_data = process_data.get("usage", {}) if "usage" in process_data else outputs.get("usage", {})
                    if usage_data:
                        node_metadata["total_tokens"] = usage_data.get("total_tokens", 0)
                        node_metadata["prompt_tokens"] = usage_data.get("prompt_tokens", 0)
                        node_metadata["completion_tokens"] = usage_data.get("completion_tokens", 0)
                elif node_execution.node_type == "dataset_retrieval":
                    span_kind = OpenInferenceSpanKindValues.RETRIEVER.value
                elif node_execution.node_type == "tool":
                    span_kind = OpenInferenceSpanKindValues.TOOL.value
                else:
                    span_kind = OpenInferenceSpanKindValues.CHAIN.value

                node_parent_span = _resolve_node_parent_span_by_execution(
                    execution_id=execution_id,
                    structured_parent_execution_id=structured_parent_execution_id,
                    span_by_execution_id=span_by_execution_id,
                    graph_parent_index=graph_parent_index,
                    workflow_span=workflow_span,
                )
                node_context = trace.set_span_in_context(node_parent_span)
                node_span = self.tracer.start_span(
                    name=_build_node_span_name(node_execution, process_data, node_metadata),
                    attributes={
                        SpanAttributes.INPUT_VALUE: node_execution.inputs or "{}",
                        SpanAttributes.INPUT_MIME_TYPE: OpenInferenceMimeTypeValues.JSON.value,
                        SpanAttributes.OUTPUT_VALUE: node_execution.outputs or "{}",
                        SpanAttributes.OUTPUT_MIME_TYPE: OpenInferenceMimeTypeValues.JSON.value,
                        SpanAttributes.OPENINFERENCE_SPAN_KIND: span_kind,
                        SpanAttributes.METADATA: json.dumps(node_metadata, ensure_ascii=False),
                        SpanAttributes.SESSION_ID: session_id,
                        **_prefixed_attributes(
                            "dify.node",
                            {
                                "execution_id": execution_id,
                                "node_execution_id": node_execution.node_execution_id,
                                "graph_id": node_execution.node_id,
                                "type": node_execution.node_type,
                                "title": node_execution.title,
                                "status": node_execution.status,
                                "tenant_id": node_execution.tenant_id,
                                "app_id": node_execution.app_id,
                                "loop_id": node_metadata.get("loop_id"),
                                "iteration_id": node_metadata.get("iteration_id"),
                            },
                        ),
                    },
                    start_time=datetime_to_nanos(created_at),
                    context=node_context,
                )
                span_by_execution_id[execution_id] = node_span

                try:
                    if (
                        node_execution.node_type == "tool"
                        and node_execution.workflow_run_id
                    ):
                        carrier: dict[str, str] = {}
                        TraceContextTextMapPropagator().inject(
                            carrier=carrier,
                            context=trace.set_span_in_context(node_span),
                        )
                        _publish_parent_span_context_aliases(node_execution, carrier)
                    if node_execution.node_type == "llm":
                        provider = process_data.get("model_provider")
                        model = process_data.get("model_name")
                        if provider:
                            node_span.set_attribute(SpanAttributes.LLM_PROVIDER, provider)
                        if model:
                            node_span.set_attribute(SpanAttributes.LLM_MODEL_NAME, model)

                        outputs = _extract_json_mapping(node_execution.outputs).get("usage", {})
                        usage_data = (
                            process_data.get("usage", {}) if "usage" in process_data else outputs.get("usage", {})
                        )
                        if usage_data:
                            node_span.set_attribute(
                                SpanAttributes.LLM_TOKEN_COUNT_TOTAL, usage_data.get("total_tokens", 0)
                            )
                            node_span.set_attribute(
                                SpanAttributes.LLM_TOKEN_COUNT_PROMPT, usage_data.get("prompt_tokens", 0)
                            )
                            node_span.set_attribute(
                                SpanAttributes.LLM_TOKEN_COUNT_COMPLETION, usage_data.get("completion_tokens", 0)
                            )
                finally:
                    _set_span_status(node_span, node_execution.error if node_execution.status != "succeeded" else None)
                    node_span.end(end_time=datetime_to_nanos(finished_at))
                return node_span

            for node_execution in workflow_nodes:
                emit_node_span(node_execution)
        finally:
            _set_span_status(workflow_span, trace_info.error)
            workflow_span.end(end_time=datetime_to_nanos(trace_info.end_time))

    def message_trace(self, trace_info: MessageTraceInfo):
        if trace_info.message_data is None:
            return

        session_id = _resolve_message_session_id(
            metadata=trace_info.metadata,
            conversation_id=trace_info.message_data.conversation_id,
        )
        file_list = cast(list[str], trace_info.file_list) or []
        message_file_data: Optional[MessageFile] = trace_info.message_file_data

        if message_file_data is not None:
            file_url = f"{self.file_base_url}/{message_file_data.url}" if message_file_data else ""
            file_list.append(file_url)

        message_metadata = {
            "message_id": trace_info.message_id or "",
            "conversation_mode": str(trace_info.conversation_mode or ""),
            "user_id": trace_info.message_data.from_account_id or "",
            "file_list": json.dumps(file_list),
            "status": trace_info.message_data.status or "",
            "status_message": trace_info.error or "",
            "level": "ERROR" if trace_info.error else "DEFAULT",
            "total_tokens": trace_info.total_tokens or 0,
            "prompt_tokens": trace_info.message_tokens or 0,
            "completion_tokens": trace_info.answer_tokens or 0,
            "ls_provider": trace_info.message_data.model_provider or "",
            "ls_model_name": trace_info.message_data.model_id or "",
        }
        message_metadata.update(trace_info.metadata)

        # Add end user data if available
        if trace_info.message_data.from_end_user_id:
            end_user_data: Optional[EndUser] = (
                db.session.query(EndUser).filter(EndUser.id == trace_info.message_data.from_end_user_id).first()
            )
            if end_user_data is not None:
                message_metadata["end_user_id"] = end_user_data.session_id

        attributes = {
            SpanAttributes.INPUT_VALUE: trace_info.message_data.query,
            SpanAttributes.OUTPUT_VALUE: trace_info.message_data.answer,
            SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
            SpanAttributes.METADATA: json.dumps(message_metadata, ensure_ascii=False),
            SpanAttributes.SESSION_ID: session_id,
        }

        trace_id = uuid_to_trace_id(trace_info.message_id)
        message_span_id = RandomIdGenerator().generate_span_id()
        span_context = SpanContext(
            trace_id=trace_id,
            span_id=message_span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

        message_span = self.tracer.start_span(
            name=TraceTaskName.MESSAGE_TRACE.value,
            attributes=attributes,
            start_time=datetime_to_nanos(trace_info.start_time),
            context=trace.set_span_in_context(trace.NonRecordingSpan(span_context)),
        )

        try:
            if trace_info.error:
                message_span.add_event(
                    "exception",
                    attributes={
                        "exception.message": trace_info.error,
                        "exception.type": "Error",
                        "exception.stacktrace": trace_info.error,
                    },
                )

            # Convert outputs to string based on type
            if isinstance(trace_info.outputs, dict | list):
                outputs_str = json.dumps(trace_info.outputs, ensure_ascii=False)
            elif isinstance(trace_info.outputs, str):
                outputs_str = trace_info.outputs
            else:
                outputs_str = str(trace_info.outputs)

            llm_attributes = {
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.LLM.value,
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.inputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_VALUE: outputs_str,
                SpanAttributes.METADATA: json.dumps(message_metadata, ensure_ascii=False),
                SpanAttributes.SESSION_ID: session_id,
            }

            if isinstance(trace_info.inputs, list):
                for i, msg in enumerate(trace_info.inputs):
                    if isinstance(msg, dict):
                        llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.{i}.message.content"] = msg.get("text", "")
                        llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.{i}.message.role"] = msg.get(
                            "role", "user"
                        )
                        # todo: handle assistant and tool role messages, as they don't always
                        # have a text field, but may have a tool_calls field instead
                        # e.g. 'tool_calls': [{'id': '98af3a29-b066-45a5-b4b1-46c74ddafc58',
                        # 'type': 'function', 'function': {'name': 'current_time', 'arguments': '{}'}}]}
            elif isinstance(trace_info.inputs, dict):
                llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.0.message.content"] = json.dumps(trace_info.inputs)
                llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.0.message.role"] = "user"
            elif isinstance(trace_info.inputs, str):
                llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.0.message.content"] = trace_info.inputs
                llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.0.message.role"] = "user"

            if trace_info.total_tokens is not None and trace_info.total_tokens > 0:
                llm_attributes[SpanAttributes.LLM_TOKEN_COUNT_TOTAL] = trace_info.total_tokens
            if trace_info.message_tokens is not None and trace_info.message_tokens > 0:
                llm_attributes[SpanAttributes.LLM_TOKEN_COUNT_PROMPT] = trace_info.message_tokens
            if trace_info.answer_tokens is not None and trace_info.answer_tokens > 0:
                llm_attributes[SpanAttributes.LLM_TOKEN_COUNT_COMPLETION] = trace_info.answer_tokens

            if trace_info.message_data.model_id is not None:
                llm_attributes[SpanAttributes.LLM_MODEL_NAME] = trace_info.message_data.model_id
            if trace_info.message_data.model_provider is not None:
                llm_attributes[SpanAttributes.LLM_PROVIDER] = trace_info.message_data.model_provider

            if trace_info.message_data and trace_info.message_data.message_metadata:
                metadata_dict = json.loads(trace_info.message_data.message_metadata)
                if model_params := metadata_dict.get("model_parameters"):
                    llm_attributes[SpanAttributes.LLM_INVOCATION_PARAMETERS] = json.dumps(model_params)

            llm_span = self.tracer.start_span(
                name="llm",
                attributes=llm_attributes,
                start_time=datetime_to_nanos(trace_info.start_time),
                context=trace.set_span_in_context(trace.NonRecordingSpan(span_context)),
            )

            try:
                if trace_info.error:
                    llm_span.add_event(
                        "exception",
                        attributes={
                            "exception.message": trace_info.error,
                            "exception.type": "Error",
                            "exception.stacktrace": trace_info.error,
                        },
                    )
            finally:
                llm_span.end(end_time=datetime_to_nanos(trace_info.end_time))
        finally:
            message_span.end(end_time=datetime_to_nanos(trace_info.end_time))

    def moderation_trace(self, trace_info: ModerationTraceInfo):
        if trace_info.message_data is None:
            return

        metadata = {
            "message_id": trace_info.message_id,
            "tool_name": "moderation",
            "status": trace_info.message_data.status,
            "status_message": trace_info.message_data.error or "",
            "level": "ERROR" if trace_info.message_data.error else "DEFAULT",
        }
        metadata.update(trace_info.metadata)

        trace_id = uuid_to_trace_id(trace_info.message_id)
        span_id = RandomIdGenerator().generate_span_id()
        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

        span = self.tracer.start_span(
            name=TraceTaskName.MODERATION_TRACE.value,
            attributes={
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.inputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_VALUE: json.dumps(
                    {
                        "action": trace_info.action,
                        "flagged": trace_info.flagged,
                        "preset_response": trace_info.preset_response,
                        "inputs": trace_info.inputs,
                    },
                    ensure_ascii=False,
                ),
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
                SpanAttributes.METADATA: json.dumps(metadata, ensure_ascii=False),
            },
            start_time=datetime_to_nanos(trace_info.start_time),
            context=trace.set_span_in_context(trace.NonRecordingSpan(context)),
        )

        try:
            if trace_info.message_data.error:
                span.add_event(
                    "exception",
                    attributes={
                        "exception.message": trace_info.message_data.error,
                        "exception.type": "Error",
                        "exception.stacktrace": trace_info.message_data.error,
                    },
                )
        finally:
            span.end(end_time=datetime_to_nanos(trace_info.end_time))

    def suggested_question_trace(self, trace_info: SuggestedQuestionTraceInfo):
        if trace_info.message_data is None:
            return

        start_time = trace_info.start_time or trace_info.message_data.created_at
        end_time = trace_info.end_time or trace_info.message_data.updated_at

        metadata = {
            "message_id": trace_info.message_id,
            "tool_name": "suggested_question",
            "status": trace_info.status,
            "status_message": trace_info.error or "",
            "level": "ERROR" if trace_info.error else "DEFAULT",
            "total_tokens": trace_info.total_tokens,
            "ls_provider": trace_info.model_provider or "",
            "ls_model_name": trace_info.model_id or "",
        }
        metadata.update(trace_info.metadata)

        trace_id = uuid_to_trace_id(trace_info.message_id)
        span_id = RandomIdGenerator().generate_span_id()
        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

        span = self.tracer.start_span(
            name=TraceTaskName.SUGGESTED_QUESTION_TRACE.value,
            attributes={
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.inputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_VALUE: json.dumps(trace_info.suggested_question, ensure_ascii=False),
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
                SpanAttributes.METADATA: json.dumps(metadata, ensure_ascii=False),
            },
            start_time=datetime_to_nanos(start_time),
            context=trace.set_span_in_context(trace.NonRecordingSpan(context)),
        )

        try:
            if trace_info.error:
                span.add_event(
                    "exception",
                    attributes={
                        "exception.message": trace_info.error,
                        "exception.type": "Error",
                        "exception.stacktrace": trace_info.error,
                    },
                )
        finally:
            span.end(end_time=datetime_to_nanos(end_time))

    def dataset_retrieval_trace(self, trace_info: DatasetRetrievalTraceInfo):
        if trace_info.message_data is None:
            return

        start_time = trace_info.start_time or trace_info.message_data.created_at
        end_time = trace_info.end_time or trace_info.message_data.updated_at

        metadata = {
            "message_id": trace_info.message_id,
            "tool_name": "dataset_retrieval",
            "status": trace_info.message_data.status,
            "status_message": trace_info.message_data.error or "",
            "level": "ERROR" if trace_info.message_data.error else "DEFAULT",
            "ls_provider": trace_info.message_data.model_provider or "",
            "ls_model_name": trace_info.message_data.model_id or "",
        }
        metadata.update(trace_info.metadata)

        trace_id = uuid_to_trace_id(trace_info.message_id)
        span_id = RandomIdGenerator().generate_span_id()
        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

        span = self.tracer.start_span(
            name=TraceTaskName.DATASET_RETRIEVAL_TRACE.value,
            attributes={
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.inputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_VALUE: json.dumps({"documents": trace_info.documents}, ensure_ascii=False),
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.RETRIEVER.value,
                SpanAttributes.METADATA: json.dumps(metadata, ensure_ascii=False),
                "start_time": start_time.isoformat() if start_time else "",
                "end_time": end_time.isoformat() if end_time else "",
            },
            start_time=datetime_to_nanos(start_time),
            context=trace.set_span_in_context(trace.NonRecordingSpan(context)),
        )

        try:
            if trace_info.message_data.error:
                span.add_event(
                    "exception",
                    attributes={
                        "exception.message": trace_info.message_data.error,
                        "exception.type": "Error",
                        "exception.stacktrace": trace_info.message_data.error,
                    },
                )
        finally:
            span.end(end_time=datetime_to_nanos(end_time))

    def tool_trace(self, trace_info: ToolTraceInfo):
        if trace_info.message_data is None:
            logger.warning("[Arize/Phoenix] Message data is None, skipping tool trace.")
            return

        metadata = {
            "message_id": trace_info.message_id,
            "tool_config": json.dumps(trace_info.tool_config, ensure_ascii=False),
        }

        trace_id = uuid_to_trace_id(trace_info.message_id)
        tool_span_id = RandomIdGenerator().generate_span_id()
        logger.info(f"[Arize/Phoenix] Creating tool trace with trace_id: {trace_id}, span_id: {tool_span_id}")

        # Create span context with the same trace_id as the parent
        # todo: Create with the appropriate parent span context, so that the tool span is
        # a child of the appropriate span (e.g. message span)
        span_context = SpanContext(
            trace_id=trace_id,
            span_id=tool_span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

        tool_params_str = (
            json.dumps(trace_info.tool_parameters, ensure_ascii=False)
            if isinstance(trace_info.tool_parameters, dict)
            else str(trace_info.tool_parameters)
        )

        span = self.tracer.start_span(
            name=trace_info.tool_name,
            attributes={
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.tool_inputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_VALUE: trace_info.tool_outputs,
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.TOOL.value,
                SpanAttributes.METADATA: json.dumps(metadata, ensure_ascii=False),
                SpanAttributes.TOOL_NAME: trace_info.tool_name,
                SpanAttributes.TOOL_PARAMETERS: tool_params_str,
            },
            start_time=datetime_to_nanos(trace_info.start_time),
            context=trace.set_span_in_context(trace.NonRecordingSpan(span_context)),
        )

        try:
            if trace_info.error:
                span.add_event(
                    "exception",
                    attributes={
                        "exception.message": trace_info.error,
                        "exception.type": "Error",
                        "exception.stacktrace": trace_info.error,
                    },
                )
        finally:
            span.end(end_time=datetime_to_nanos(trace_info.end_time))

    def generate_name_trace(self, trace_info: GenerateNameTraceInfo):
        if trace_info.message_data is None:
            return

        session_id = _resolve_message_session_id(
            metadata=trace_info.metadata,
            conversation_id=trace_info.message_data.conversation_id,
        )
        metadata = {
            "project_name": self.project,
            "message_id": trace_info.message_id,
            "status": trace_info.message_data.status,
            "status_message": trace_info.message_data.error or "",
            "level": "ERROR" if trace_info.message_data.error else "DEFAULT",
        }
        metadata.update(trace_info.metadata)

        trace_id = uuid_to_trace_id(trace_info.message_id)
        span_id = RandomIdGenerator().generate_span_id()
        context = SpanContext(
            trace_id=trace_id,
            span_id=span_id,
            is_remote=False,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
            trace_state=TraceState(),
        )

        span = self.tracer.start_span(
            name=TraceTaskName.GENERATE_NAME_TRACE.value,
            attributes={
                SpanAttributes.INPUT_VALUE: json.dumps(trace_info.inputs, ensure_ascii=False),
                SpanAttributes.OUTPUT_VALUE: json.dumps(trace_info.outputs, ensure_ascii=False),
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
                SpanAttributes.METADATA: json.dumps(metadata, ensure_ascii=False),
                SpanAttributes.SESSION_ID: session_id,
                "start_time": trace_info.start_time.isoformat() if trace_info.start_time else "",
                "end_time": trace_info.end_time.isoformat() if trace_info.end_time else "",
            },
            start_time=datetime_to_nanos(trace_info.start_time),
            context=trace.set_span_in_context(trace.NonRecordingSpan(context)),
        )

        try:
            if trace_info.message_data.error:
                span.add_event(
                    "exception",
                    attributes={
                        "exception.message": trace_info.message_data.error,
                        "exception.type": "Error",
                        "exception.stacktrace": trace_info.message_data.error,
                    },
                )
        finally:
            span.end(end_time=datetime_to_nanos(trace_info.end_time))

    def api_check(self):
        try:
            with self.tracer.start_span("api_check") as span:
                span.set_attribute("test", "true")
            return True
        except Exception as e:
            logger.info(f"[Arize/Phoenix] API check failed: {str(e)}", exc_info=True)
            raise ValueError(f"[Arize/Phoenix] API check failed: {str(e)}")

    def get_project_url(self):
        try:
            if self.arize_phoenix_config.endpoint == "https://otlp.arize.com":
                return "https://app.arize.com/"
            else:
                return f"{self.arize_phoenix_config.endpoint}/projects/"
        except Exception as e:
            logger.info(f"[Arize/Phoenix] Get run url failed: {str(e)}", exc_info=True)
            raise ValueError(f"[Arize/Phoenix] Get run url failed: {str(e)}")

    def ensure_root_span(
        self,
        dify_trace_id: str | None,
        *,
        root_span_name: str | None = None,
        start_time: int | None = None,
        end_time: int | None = None,
        root_span_attributes: Mapping[str, Any] | None = None,
    ) -> dict[str, str]:
        trace_key = str(dify_trace_id)
        if trace_key not in self.dify_trace_ids:
            carrier: dict[str, str] = {}
            span_name = root_span_name.strip() if isinstance(root_span_name, str) and root_span_name.strip() else "Dify"
            attributes: dict[str, Any] = {
                SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
                "dify_project_name": str(self.project),
                "dify_trace_id": trace_key,
            }
            if root_span_attributes:
                attributes.update(root_span_attributes)

            root_span = self.tracer.start_span(
                name=span_name,
                attributes=attributes,
                start_time=start_time,
                context=Context(),
            )
            with use_span(root_span, end_on_exit=False):
                self.propagator.inject(carrier=carrier)
            _set_span_status(root_span)
            root_span.end(end_time=end_time)

            self.dify_trace_ids.add(trace_key)
            self.root_span_carriers[trace_key] = carrier

        self.carrier = self.root_span_carriers[trace_key]
        return self.carrier

    def _get_workflow_nodes(self, workflow_run_id: str):
        """Helper method to get workflow nodes"""
        workflow_nodes = (
            db.session.query(
                WorkflowNodeExecutionModel.id,
                WorkflowNodeExecutionModel.tenant_id,
                WorkflowNodeExecutionModel.app_id,
                WorkflowNodeExecutionModel.workflow_run_id,
                WorkflowNodeExecutionModel.predecessor_node_id,
                WorkflowNodeExecutionModel.node_execution_id,
                WorkflowNodeExecutionModel.node_id,
                WorkflowNodeExecutionModel.title,
                WorkflowNodeExecutionModel.node_type,
                WorkflowNodeExecutionModel.status,
                WorkflowNodeExecutionModel.error,
                WorkflowNodeExecutionModel.inputs,
                WorkflowNodeExecutionModel.outputs,
                WorkflowNodeExecutionModel.created_at,
                WorkflowNodeExecutionModel.elapsed_time,
                WorkflowNodeExecutionModel.process_data,
                WorkflowNodeExecutionModel.execution_metadata,
            )
            .filter(WorkflowNodeExecutionModel.workflow_run_id == workflow_run_id)
            .all()
        )
        return workflow_nodes
