import json
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from openinference.semconv.trace import OpenInferenceMimeTypeValues, SpanAttributes
from opentelemetry import trace
from opentelemetry.sdk import trace as trace_sdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import NonRecordingSpan, SpanContext, StatusCode, TraceFlags, TraceState, use_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from core.ops.arize_phoenix_trace.arize_phoenix_trace import (
    ArizePhoenixDataTrace,
    _app_uses_phoenix_provider,
    _build_parent_span_bridge_id,
    _build_wrapper_groups,
    _normalize_wrapper_index,
    _parent_workflow_can_publish_span_context,
    _resolve_node_parent_span,
    _resolve_published_parent_span_context,
    _resolve_session_id,
    datetime_to_nanos,
)
from core.ops.entities.trace_entity import GenerateNameTraceInfo, MessageTraceInfo, WorkflowTraceInfo
from core.ops.exceptions import PendingTraceParentContextError


class _FakeSpan:
    def __init__(self, name, attributes, start_time=None):
        self.name = name
        self.attributes = attributes
        self.start_time = start_time
        self.end_time = None
        self.ended = False
        self.end_count = 0
        self.status = None
        self.events = []

    def get_span_context(self):
        return trace.INVALID_SPAN_CONTEXT

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def set_status(self, status):
        self.status = status

    def add_event(self, name, attributes=None):
        self.events.append((name, attributes or {}))

    def end(self, end_time=None):
        self.end_time = end_time
        self.ended = True
        self.end_count += 1


class _FakeTracer:
    def __init__(self):
        self.spans = []

    def start_span(self, name, attributes=None, **kwargs):
        span = _FakeSpan(name, attributes or {}, kwargs.get("start_time"))
        context = kwargs.get("context")
        parent = context if isinstance(context, _FakeSpan) else trace.get_current_span(context)
        span.parent_name = parent.name if isinstance(parent, _FakeSpan) else None
        self.spans.append(span)
        return span


class _CollectingSpanExporter(SpanExporter):
    def __init__(self):
        self.spans = []

    def export(self, spans):
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self):
        return None


class _FakeQuery:
    def __init__(self, result):
        self._result = result

    def filter(self, *args, **kwargs):
        return self

    def first(self):
        return self._result


def _make_trace_instance(monkeypatch, nodes=None):
    tracer = _FakeTracer()
    instance = ArizePhoenixDataTrace.__new__(ArizePhoenixDataTrace)
    instance.tracer = tracer
    instance.project = "test"
    instance.file_base_url = "http://files"
    instance.propagator = SimpleNamespace(
        inject=lambda carrier, context=None: carrier.update({"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}),
        extract=lambda carrier: None,
    )
    instance.dify_trace_ids = set()
    instance.root_span_carriers = {}
    instance.carrier = {}
    monkeypatch.setattr(instance, "_get_workflow_nodes", lambda workflow_run_id: nodes or [])
    return instance, tracer


def _make_workflow_trace_info(**overrides):
    values = {
        "message_id": None,
        "message_data": None,
        "workflow_data": None,
        "conversation_id": None,
        "workflow_app_log_id": None,
        "workflow_id": "workflow-id",
        "tenant_id": "tenant-id",
        "workflow_run_id": "workflow-run-123456",
        "workflow_run_elapsed_time": 1.0,
        "workflow_run_status": "succeeded",
        "workflow_run_inputs": {"query": "hello"},
        "workflow_run_outputs": {"answer": "world"},
        "workflow_run_version": "1",
        "error": None,
        "total_tokens": 0,
        "file_list": [],
        "query": "hello",
        "metadata": {"app_name": "Root Chat"},
        "start_time": datetime(2026, 1, 1, 0, 0, 0),
        "end_time": datetime(2026, 1, 1, 0, 0, 1),
    }
    values.update(overrides)
    return WorkflowTraceInfo(**values)


def _make_node_execution(**overrides):
    values = {
        "id": "node-exec-1",
        "tenant_id": "tenant-id",
        "app_id": "app-id",
        "workflow_run_id": "workflow-run-123456",
        "predecessor_node_id": None,
        "node_execution_id": "node-exec-1",
        "node_id": "llm-node",
        "title": "LLM",
        "node_type": "llm",
        "status": "succeeded",
        "error": None,
        "inputs": "{}",
        "outputs": "{}",
        "created_at": datetime(2026, 1, 1, 0, 0, 0),
        "elapsed_time": 1.0,
        "process_data": json.dumps({"model_provider": "openai", "model_name": "gpt"}),
        "execution_metadata": "{}",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _make_message_trace_info(**overrides):
    message_data = SimpleNamespace(
        query="hello",
        answer="world",
        from_account_id="account-id",
        from_end_user_id=None,
        status="normal",
        error=None,
        model_provider="openai",
        model_id="gpt-4o",
        conversation_id="conversation-id",
        message_metadata=None,
    )
    values = {
        "message_id": "message-id",
        "message_data": message_data,
        "inputs": "hello",
        "outputs": "world",
        "start_time": datetime(2026, 1, 1, 0, 0, 0),
        "end_time": datetime(2026, 1, 1, 0, 0, 1),
        "metadata": {},
        "conversation_model": "chat",
        "message_tokens": 1,
        "answer_tokens": 2,
        "total_tokens": 3,
        "error": None,
        "file_list": [],
        "message_file_data": None,
        "conversation_mode": "chat",
    }
    values.update(overrides)
    return MessageTraceInfo(**values)


def _make_generate_name_trace_info(**overrides):
    message_data = SimpleNamespace(
        status="normal",
        error=None,
        conversation_id="conversation-id",
    )
    values = {
        "message_id": "message-id",
        "message_data": message_data,
        "inputs": {"query": "hello"},
        "outputs": {"name": "Greeting"},
        "start_time": datetime(2026, 1, 1, 0, 0, 0),
        "end_time": datetime(2026, 1, 1, 0, 0, 1),
        "metadata": {},
        "conversation_id": "conversation-id",
        "tenant_id": "tenant-id",
    }
    values.update(overrides)
    return GenerateNameTraceInfo(**values)


def test_resolve_session_id_prefers_trace_session_id():
    assert (
        _resolve_session_id(
            trace_session_id="custom-session",
            conversation_id="conversation-id",
            workflow_run_id="child-run",
            parent_workflow_run_id="outer-run",
        )
        == "custom-session"
    )


def test_resolve_session_id_prefers_conversation_id():
    assert (
        _resolve_session_id(
            trace_session_id=None,
            conversation_id="conversation-id",
            workflow_run_id="child-run",
            parent_workflow_run_id="outer-run",
        )
        == "conversation-id"
    )


def test_resolve_session_id_uses_parent_workflow_for_nested_workflow():
    assert (
        _resolve_session_id(
            trace_session_id=None,
            conversation_id=None,
            workflow_run_id="child-run",
            parent_workflow_run_id="outer-run",
        )
        == "outer-run"
    )


def test_resolve_session_id_uses_workflow_run_for_top_level_workflow():
    assert (
        _resolve_session_id(
            trace_session_id=None,
            conversation_id=None,
            workflow_run_id="workflow-run",
            parent_workflow_run_id=None,
        )
        == "workflow-run"
    )


def test_build_parent_span_bridge_id_uses_workflow_run_and_node_id():
    assert _build_parent_span_bridge_id("outer-run", "tool-node") == "outer-run:tool-node"


def test_normalize_wrapper_index_accepts_stable_values():
    assert _normalize_wrapper_index(0) == "0"
    assert _normalize_wrapper_index(12) == "12"
    assert _normalize_wrapper_index("01") == "01"
    assert _normalize_wrapper_index("branch-1_A.2:3") == "branch-1_A.2:3"


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        -1,
        1.0,
        "",
        " 1",
        "1 ",
        "group/1",
        "group]1",
        None,
    ],
)
def test_normalize_wrapper_index_rejects_unstable_values(value):
    assert _normalize_wrapper_index(value) is None


def test_missing_parent_span_context_raises_retryable_error(monkeypatch):
    monkeypatch.setattr("core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get", lambda key: None)

    with pytest.raises(PendingTraceParentContextError):
        _resolve_published_parent_span_context("outer-run:tool-node")


def test_app_uses_phoenix_provider_only_for_enabled_arize_or_phoenix():
    assert _app_uses_phoenix_provider({"enabled": True, "tracing_provider": "phoenix"}) is True
    assert _app_uses_phoenix_provider({"enabled": True, "tracing_provider": "arize"}) is True
    assert _app_uses_phoenix_provider({"enabled": False, "tracing_provider": "phoenix"}) is False
    assert _app_uses_phoenix_provider({"enabled": True, "tracing_provider": "langfuse"}) is False
    assert _app_uses_phoenix_provider(None) is False


def test_parent_workflow_can_publish_span_context_keeps_unknown_parent_retryable(monkeypatch):
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.db.session.query",
        lambda model: _FakeQuery(None),
    )

    assert _parent_workflow_can_publish_span_context("missing-run") is True


def test_parent_workflow_can_publish_span_context_checks_parent_app_tracing(monkeypatch):
    parent_run = SimpleNamespace(app_id="parent-app")
    parent_app = SimpleNamespace(tracing=json.dumps({"enabled": True, "tracing_provider": "phoenix"}))

    def fake_query(model):
        if getattr(model, "__tablename__", None) == "workflow_runs":
            return _FakeQuery(parent_run)
        if getattr(model, "__tablename__", None) == "apps":
            return _FakeQuery(parent_app)
        raise AssertionError(f"Unexpected model query: {model}")

    monkeypatch.setattr("core.ops.arize_phoenix_trace.arize_phoenix_trace.db.session.query", fake_query)

    assert _parent_workflow_can_publish_span_context("parent-run") is True

    parent_app.tracing = json.dumps({"enabled": False, "tracing_provider": "phoenix"})
    assert _parent_workflow_can_publish_span_context("parent-run") is False

    parent_app.tracing = json.dumps({"enabled": True, "tracing_provider": "langfuse"})
    assert _parent_workflow_can_publish_span_context("parent-run") is False


def test_invalid_parent_span_context_rejected(monkeypatch):
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get",
        lambda key: json.dumps({"traceparent": "invalid"}),
    )

    with pytest.raises(ValueError):
        _resolve_published_parent_span_context("outer-run:tool-node")


def test_workflow_trace_creates_root_span_without_message_data(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)

    instance.workflow_trace(_make_workflow_trace_info())

    assert len(tracer.spans) == 2
    assert tracer.spans[0].name == "workflow-run-123456"
    assert tracer.spans[1].name == "Root_Chat_workflow"
    assert tracer.spans[1].attributes[SpanAttributes.SESSION_ID] == "workflow-run-123456"
    assert tracer.spans[1].attributes[SpanAttributes.INPUT_MIME_TYPE] == OpenInferenceMimeTypeValues.JSON.value
    assert tracer.spans[1].attributes[SpanAttributes.OUTPUT_MIME_TYPE] == OpenInferenceMimeTypeValues.JSON.value
    assert tracer.spans[1].status.status_code == StatusCode.OK


def test_workflow_trace_records_workflow_error_as_exception_event(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)

    instance.workflow_trace(
        _make_workflow_trace_info(
            workflow_run_status="failed",
            error="Traceback (most recent call last):\nNameError: name 'missing' is not defined",
        )
    )

    root_span = tracer.spans[0]
    workflow_span = tracer.spans[1]
    assert root_span.status.status_code == StatusCode.ERROR
    assert root_span.events == [
        (
            "exception",
            {
                "exception.message": "Traceback (most recent call last):\nNameError: name 'missing' is not defined",
                "exception.type": "Error",
                "exception.stacktrace": "Traceback (most recent call last):\nNameError: name 'missing' is not defined",
            },
        )
    ]
    assert workflow_span.status.status_code == StatusCode.ERROR
    assert workflow_span.events == [
        (
            "exception",
            {
                "exception.message": "Traceback (most recent call last):\nNameError: name 'missing' is not defined",
                "exception.type": "Error",
                "exception.stacktrace": "Traceback (most recent call last):\nNameError: name 'missing' is not defined",
            },
        )
    ]


def test_workflow_trace_records_failed_node_error_as_exception_event(monkeypatch):
    failed_node = _make_node_execution(
        status="failed",
        error="Traceback (most recent call last):\nRuntimeError: node failed",
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[failed_node])

    instance.workflow_trace(_make_workflow_trace_info())

    node_span = next(span for span in tracer.spans if span.name == "llm_gpt")
    assert node_span.status.status_code == StatusCode.ERROR
    assert node_span.events == [
        (
            "exception",
            {
                "exception.message": "Traceback (most recent call last):\nRuntimeError: node failed",
                "exception.type": "Error",
                "exception.stacktrace": "Traceback (most recent call last):\nRuntimeError: node failed",
            },
        )
    ]


def test_workflow_trace_uses_trace_session_id_for_root_workflow_and_node_spans(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[_make_node_execution()])

    instance.workflow_trace(
        _make_workflow_trace_info(
            conversation_id="conversation-id",
            metadata={
                "app_name": "Root Chat",
                "trace_session_id": "custom-session",
            },
        )
    )

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "llm_gpt",
    ]
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "custom-session"
    assert tracer.spans[1].attributes[SpanAttributes.SESSION_ID] == "custom-session"
    assert tracer.spans[2].attributes[SpanAttributes.SESSION_ID] == "custom-session"


def test_nested_workflow_trace_uses_published_parent_context(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._resolve_published_parent_span_context",
        lambda parent_node_execution_id: {
            "traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01",
        },
    )

    instance.workflow_trace(
        _make_workflow_trace_info(
            workflow_run_id="child-run-123456",
            metadata={
                "app_name": "Child Workflow",
                "parent_trace_context": {
                    "parent_workflow_run_id": "outer-run",
                    "parent_node_execution_id": "outer-run:tool-exec-id",
                },
            },
        )
    )

    assert tracer.spans[0].name == "nested_Child_Workflow_child-ru"
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "outer-run"


def test_nested_workflow_trace_falls_back_when_parent_app_tracing_disabled(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get",
        lambda key: None,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._parent_workflow_can_publish_span_context",
        lambda parent_workflow_run_id: False,
    )

    instance.workflow_trace(
        _make_workflow_trace_info(
            workflow_run_id="child-run-123456",
            metadata={
                "app_name": "Child Workflow",
                "parent_trace_context": {
                    "parent_workflow_run_id": "outer-run",
                    "parent_node_execution_id": "outer-run:tool-exec-id",
                },
            },
        )
    )

    assert [span.name for span in tracer.spans] == ["child-run-123456", "nested_Child_Workflow_child-ru"]
    assert tracer.spans[1].attributes[SpanAttributes.SESSION_ID] == "outer-run"


def test_nested_workflow_trace_still_retries_when_parent_app_can_publish_context(monkeypatch):
    instance, _ = _make_trace_instance(monkeypatch)
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get",
        lambda key: None,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._parent_workflow_can_publish_span_context",
        lambda parent_workflow_run_id: True,
    )

    with pytest.raises(PendingTraceParentContextError):
        instance.workflow_trace(
            _make_workflow_trace_info(
                workflow_run_id="child-run-123456",
                metadata={
                    "app_name": "Child Workflow",
                    "parent_trace_context": {
                        "parent_workflow_run_id": "outer-run",
                        "parent_node_execution_id": "outer-run:tool-exec-id",
                    },
                },
            )
        )


def test_nested_workflow_trace_keeps_parent_carrier_and_prefers_trace_session_id(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)
    resolved_parent_node_execution_ids = []
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._resolve_published_parent_span_context",
        lambda parent_node_execution_id: resolved_parent_node_execution_ids.append(parent_node_execution_id)
        or {
            "traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01",
        },
    )

    instance.workflow_trace(
        _make_workflow_trace_info(
            workflow_run_id="child-run-123456",
            metadata={
                "app_name": "Child Workflow",
                "trace_session_id": "custom-session",
                "parent_trace_context": {
                    "parent_workflow_run_id": "outer-run",
                    "parent_node_execution_id": "outer-run:tool-exec-id",
                },
            },
        )
    )

    assert resolved_parent_node_execution_ids == ["outer-run:tool-exec-id"]
    assert tracer.spans[0].name == "nested_Child_Workflow_child-ru"
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "custom-session"


def test_message_trace_uses_trace_session_id_for_message_and_llm_spans(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)

    instance.message_trace(_make_message_trace_info(metadata={"trace_session_id": "custom-session"}))

    assert [span.name for span in tracer.spans] == ["message", "llm"]
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "custom-session"
    assert tracer.spans[1].attributes[SpanAttributes.SESSION_ID] == "custom-session"


def test_message_trace_falls_back_to_conversation_id_without_valid_trace_session_id(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)

    instance.message_trace(_make_message_trace_info(metadata={"trace_session_id": ""}))

    assert [span.name for span in tracer.spans] == ["message", "llm"]
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "conversation-id"
    assert tracer.spans[1].attributes[SpanAttributes.SESSION_ID] == "conversation-id"


def test_generate_name_trace_uses_trace_session_id(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)

    instance.generate_name_trace(_make_generate_name_trace_info(metadata={"trace_session_id": "custom-session"}))

    assert [span.name for span in tracer.spans] == ["generate_conversation_name"]
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "custom-session"


def test_generate_name_trace_falls_back_to_conversation_id_without_valid_trace_session_id(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)

    instance.generate_name_trace(_make_generate_name_trace_info(metadata={"trace_session_id": ""}))

    assert [span.name for span in tracer.spans] == ["generate_conversation_name"]
    assert tracer.spans[0].attributes[SpanAttributes.SESSION_ID] == "conversation-id"


def test_workflow_root_span_uses_workflow_time_bounds(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)
    trace_info = _make_workflow_trace_info(
        start_time=datetime(2026, 1, 1, 0, 0, 3),
        end_time=datetime(2026, 1, 1, 0, 0, 9),
    )

    instance.workflow_trace(trace_info)

    assert tracer.spans[0].name == "workflow-run-123456"
    assert tracer.spans[0].start_time == datetime_to_nanos(trace_info.start_time)
    assert tracer.spans[0].end_time == datetime_to_nanos(trace_info.end_time)


def test_root_span_ignores_unsampled_ambient_otel_parent():
    exporter = _CollectingSpanExporter()
    provider = trace_sdk.TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    instance = ArizePhoenixDataTrace.__new__(ArizePhoenixDataTrace)
    instance.tracer = provider.get_tracer("test")
    instance.project = "test"
    instance.propagator = TraceContextTextMapPropagator()
    instance.dify_trace_ids = set()
    instance.root_span_carriers = {}
    instance.carrier = {}

    unsampled_context = SpanContext(
        trace_id=1,
        span_id=2,
        is_remote=False,
        trace_flags=TraceFlags(0),
        trace_state=TraceState(),
    )

    with use_span(NonRecordingSpan(unsampled_context), end_on_exit=False):
        instance.ensure_root_span("workflow-run-123456", root_span_name="workflow-run-123456")

    assert [span.name for span in exporter.spans] == ["workflow-run-123456"]
    assert exporter.spans[0].context.trace_flags.sampled


def test_workflow_trace_ignores_malformed_llm_outputs(monkeypatch):
    node = SimpleNamespace(
        id="llm-row-id",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id=None,
        node_execution_id="llm-exec-id",
        node_id="llm-node",
        title="LLM",
        node_type="llm",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{not-json",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        elapsed_time=1.0,
        process_data=json.dumps({"model_provider": "openai", "model_name": "gpt"}),
        execution_metadata="{}",
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[node])

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "llm_gpt",
    ]


def test_workflow_trace_uses_llm_process_prompts_as_llm_input(monkeypatch):
    prompts = [
        {"role": "system", "text": "You are concise."},
        {"role": "user", "text": "hi"},
    ]
    node = _make_node_execution(
        inputs="{}",
        process_data=json.dumps(
            {
                "model_provider": "openai",
                "model_name": "gpt",
                "prompts": prompts,
            }
        ),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[node])

    instance.workflow_trace(_make_workflow_trace_info())

    llm_attributes = tracer.spans[2].attributes
    assert llm_attributes[SpanAttributes.INPUT_VALUE] == json.dumps(prompts, ensure_ascii=False)
    assert llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.0.message.role"] == "system"
    assert llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.0.message.content"] == "You are concise."
    assert llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.1.message.role"] == "user"
    assert llm_attributes[f"{SpanAttributes.LLM_INPUT_MESSAGES}.1.message.content"] == "hi"


def test_workflow_trace_uses_source_style_node_names(monkeypatch):
    node = SimpleNamespace(
        id="node-exec-1",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id=None,
        node_execution_id="node-exec-1",
        node_id="tool-node",
        title="Call Child",
        node_type="tool",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        elapsed_time=1.0,
        process_data="{}",
        execution_metadata=json.dumps({"tool_info": {"tool_name": "child_tool"}}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[node])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._publish_parent_span_context",
        lambda parent_node_execution_id, carrier: None,
    )

    instance.workflow_trace(_make_workflow_trace_info(conversation_id="conversation-id"))

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "tool_Call_Child_[child_tool]",
    ]
    assert tracer.spans[2].attributes[SpanAttributes.SESSION_ID] == "conversation-id"
    assert tracer.spans[2].attributes[SpanAttributes.INPUT_MIME_TYPE] == OpenInferenceMimeTypeValues.JSON.value
    assert tracer.spans[2].attributes[SpanAttributes.OUTPUT_MIME_TYPE] == OpenInferenceMimeTypeValues.JSON.value
    assert tracer.spans[2].status.status_code == StatusCode.OK


def test_workflow_trace_exposes_node_metadata_as_queryable_attributes(monkeypatch):
    node = SimpleNamespace(
        id="node-exec-1",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id=None,
        node_execution_id="node-exec-1",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        elapsed_time=1.0,
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node"}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[node])

    instance.workflow_trace(_make_workflow_trace_info())

    node_attributes = tracer.spans[2].attributes
    assert json.loads(node_attributes[SpanAttributes.METADATA])["loop_id"] == "loop-node"
    assert node_attributes["dify.node.execution_id"] == "node-exec-1"
    assert node_attributes["dify.node.graph_id"] == "template-node"
    assert node_attributes["dify.node.type"] == "template-transform"
    assert node_attributes["dify.node.title"] == "Template"
    assert node_attributes["dify.node.loop_id"] == "loop-node"


def test_workflow_trace_publishes_parent_span_aliases_for_tool_nodes(monkeypatch):
    node = SimpleNamespace(
        id="db-row-id",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id=None,
        node_execution_id=None,
        node_id="graph-tool-node",
        title="Call Child",
        node_type="tool",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        elapsed_time=1.0,
        process_data="{}",
        execution_metadata=json.dumps({"tool_info": {"provider_type": "workflow"}}),
    )
    published_keys = []
    instance, _ = _make_trace_instance(monkeypatch, nodes=[node])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._publish_parent_span_context",
        lambda parent_node_execution_id, carrier: published_keys.append(parent_node_execution_id),
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert published_keys == [
        "workflow-run-123456:db-row-id",
        "workflow-run-123456:graph-tool-node",
    ]


def test_workflow_trace_tool_inside_wrapper_publishes_tool_span_carrier(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    tool = _make_node_execution(
        id="tool-row-id",
        node_execution_id=None,
        node_id="graph-tool-node",
        title="Call Child",
        node_type="tool",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    published_contexts = []
    published_keys = []
    published_carriers = []
    instance, _ = _make_trace_instance(monkeypatch, nodes=[loop, tool])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.TraceContextTextMapPropagator",
        lambda: SimpleNamespace(
            inject=lambda carrier, context=None: published_contexts.append(getattr(context, "name", None))
            or carrier.update({"traceparent": "fake"})
        ),
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._publish_parent_span_context",
        lambda parent_node_execution_id, carrier: published_keys.append(parent_node_execution_id)
        or published_carriers.append(dict(carrier)),
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert published_contexts == ["tool_Call_Child_tool"]
    assert published_keys == [
        "workflow-run-123456:tool-row-id",
        "workflow-run-123456:graph-tool-node",
    ]
    assert published_carriers == [{"traceparent": "fake"}, {"traceparent": "fake"}]


def test_workflow_trace_keeps_sequential_nodes_as_workflow_children(monkeypatch):
    start = _make_node_execution(
        id="start-row-id",
        node_execution_id="start-exec-id",
        node_id="start-node",
        title="START",
        node_type="start",
        process_data="{}",
    )
    llm = _make_node_execution(
        id="llm-row-id",
        node_execution_id="llm-exec-id",
        node_id="llm-node",
        predecessor_node_id="start-node",
        title="LLM",
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[start, llm])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "start",
        "llm_gpt",
    ]
    assert tracer.spans[2].parent_name == "Root_Chat_workflow"
    assert tracer.spans[3].parent_name == "Root_Chat_workflow"


def test_build_wrapper_groups_groups_loop_children_by_index():
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    first = _make_node_execution(
        id="template-row-id-0",
        node_execution_id="template-exec-id-0",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    second = _make_node_execution(
        id="template-row-id-1",
        node_execution_id="template-exec-id-1",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 1}),
    )

    groups = _build_wrapper_groups([loop, first, second])

    assert [(group.key.wrapper_type, group.key.index) for group in groups.values()] == [
        ("loop", "0"),
        ("loop", "1"),
    ]
    assert [group.container_execution_id for group in groups.values()] == ["loop-row-id", "loop-row-id"]
    assert [group.child_execution_ids for group in groups.values()] == [
        {"template-row-id-0"},
        {"template-row-id-1"},
    ]


def test_build_wrapper_groups_skips_ambiguous_container_graph_ids():
    first_loop = _make_node_execution(
        id="loop-row-id-1",
        node_execution_id="loop-exec-id-1",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    second_loop = _make_node_execution(
        id="loop-row-id-2",
        node_execution_id="loop-exec-id-2",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    child = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )

    assert _build_wrapper_groups([first_loop, second_loop, child]) == {}


def test_workflow_trace_parents_container_children_to_loop_when_predecessor_missing(monkeypatch):
    loop = SimpleNamespace(
        id="loop-row-id",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id="start-node",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 1),
        elapsed_time=1.0,
        process_data="{}",
        execution_metadata="{}",
    )
    template = SimpleNamespace(
        id="template-row-id",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id="loop-start-node",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 2),
        elapsed_time=1.0,
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node"}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, template])

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "loop_main_loop",
        "template-transform_Template",
    ]
    assert (
        _resolve_node_parent_span(
            template,
            {"loop_id": "loop-node"},
            {"loop-node": tracer.spans[2]},
            tracer.spans[0],
        )
        is tracer.spans[2]
    )


def test_workflow_trace_groups_repeated_loop_body_nodes_by_index(monkeypatch):
    loop = SimpleNamespace(
        id="loop-row-id",
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_run_id="workflow-run-123456",
        predecessor_node_id="start-node",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        status="succeeded",
        error=None,
        inputs="{}",
        outputs="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 1),
        elapsed_time=1.0,
        process_data="{}",
        execution_metadata="{}",
    )

    nodes = [loop]
    for index in range(3):
        nodes.extend(
            [
                SimpleNamespace(
                    id=f"template-row-id-{index}",
                    tenant_id="tenant-id",
                    app_id="app-id",
                    workflow_run_id="workflow-run-123456",
                    predecessor_node_id="loop-start-node",
                    node_execution_id=f"template-exec-id-{index}",
                    node_id="template-node",
                    title="Template",
                    node_type="template-transform",
                    status="succeeded",
                    error=None,
                    inputs="{}",
                    outputs="{}",
                    created_at=datetime(2026, 1, 1, 0, 0, 2 + index),
                    elapsed_time=1.0,
                    process_data="{}",
                    execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": index}),
                ),
                SimpleNamespace(
                    id=f"tool-row-id-{index}",
                    tenant_id="tenant-id",
                    app_id="app-id",
                    workflow_run_id="workflow-run-123456",
                    predecessor_node_id="template-node",
                    node_execution_id=f"tool-exec-id-{index}",
                    node_id="tool-node",
                    title="Embedded Workflow 2",
                    node_type="tool",
                    status="succeeded",
                    error=None,
                    inputs="{}",
                    outputs="{}",
                    created_at=datetime(2026, 1, 1, 0, 0, 3 + index),
                    elapsed_time=1.0,
                    process_data="{}",
                    execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": index}),
                ),
                SimpleNamespace(
                    id=f"assigner-row-id-{index}",
                    tenant_id="tenant-id",
                    app_id="app-id",
                    workflow_run_id="workflow-run-123456",
                    predecessor_node_id="tool-node",
                    node_execution_id=f"assigner-exec-id-{index}",
                    node_id="assigner-node",
                    title="Variable Assigner",
                    node_type="assigner",
                    status="succeeded",
                    error=None,
                    inputs="{}",
                    outputs="{}",
                    created_at=datetime(2026, 1, 1, 0, 0, 4 + index),
                    elapsed_time=1.0,
                    process_data="{}",
                    execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": index}),
                ),
            ]
        )

    instance, tracer = _make_trace_instance(monkeypatch, nodes=nodes)
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.TraceContextTextMapPropagator",
        lambda: SimpleNamespace(inject=lambda carrier, context=None: carrier.update({"traceparent": "fake"})),
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._publish_parent_span_context",
        lambda parent_node_execution_id, carrier: None,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    wrapper_spans = [span for span in tracer.spans if span.name.startswith("loop[")]
    assert [span.name for span in wrapper_spans] == ["loop[0]", "loop[1]", "loop[2]"]
    assert {span.parent_name for span in wrapper_spans} == {"loop_main_loop"}

    body_spans = [
        span
        for span in tracer.spans
        if span.name
        in {
            "template-transform_Template",
            "tool_Embedded_Workflow_2_tool",
            "assigner_Variable_Assigner",
        }
    ]
    assert len(body_spans) == 9
    assert {span.attributes["dify.node.execution_id"]: span.parent_name for span in body_spans} == {
        "template-row-id-0": "loop[0]",
        "tool-row-id-0": "loop[0]",
        "assigner-row-id-0": "loop[0]",
        "template-row-id-1": "loop[1]",
        "tool-row-id-1": "loop[1]",
        "assigner-row-id-1": "loop[1]",
        "template-row-id-2": "loop[2]",
        "tool-row-id-2": "loop[2]",
        "assigner-row-id-2": "loop[2]",
    }


def test_workflow_trace_groups_iteration_body_nodes_by_index(monkeypatch):
    iteration = _make_node_execution(
        id="iteration-row-id",
        node_execution_id="iteration-exec-id",
        node_id="iteration-node",
        title="Iteration",
        node_type="iteration",
        process_data="{}",
    )
    first = _make_node_execution(
        id="if-row-id-0",
        node_execution_id="if-exec-id-0",
        node_id="if-node",
        title="IF/ELSE",
        node_type="if-else",
        process_data="{}",
        execution_metadata=json.dumps({"iteration_id": "iteration-node", "iteration_index": 0}),
    )
    second = _make_node_execution(
        id="template-row-id-1",
        node_execution_id="template-exec-id-1",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"iteration_id": "iteration-node", "iteration_index": 1}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[iteration, first, second])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "iteration",
        "iteration[0]",
        "if-else_IF/ELSE_condition",
        "iteration[1]",
        "template-transform_Template",
    ]
    assert tracer.spans[3].parent_name == "iteration"
    assert tracer.spans[4].parent_name == "iteration[0]"
    assert tracer.spans[5].parent_name == "iteration"
    assert tracer.spans[6].parent_name == "iteration[1]"
    assert tracer.spans[4].attributes["dify.node.iteration_index"] == 0
    assert tracer.spans[6].attributes["dify.node.iteration_index"] == 1


def test_workflow_trace_exposes_loop_index_as_queryable_node_attribute(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    child = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 3}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, child])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    child_span = next(span for span in tracer.spans if span.name == "template-transform_Template")
    assert child_span.attributes["dify.node.loop_index"] == 3

    wrapper = next(span for span in tracer.spans if span.name == "loop[3]")
    wrapper_metadata = json.loads(wrapper.attributes[SpanAttributes.METADATA])
    assert wrapper.attributes[SpanAttributes.SESSION_ID] == "workflow-run-123456"
    assert wrapper.attributes["dify.wrapper.synthetic"] is True
    assert wrapper.attributes["dify.wrapper.type"] == "loop"
    assert wrapper.attributes["dify.wrapper.index"] == "3"
    assert wrapper.attributes["dify.wrapper.container_execution_id"] == "loop-row-id"
    assert wrapper_metadata == {
        "synthetic": True,
        "wrapper_type": "loop",
        "wrapper_index": "3",
        "container_execution_id": "loop-row-id",
    }


def test_workflow_trace_wrapper_uses_child_time_bounds_and_error_status(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        elapsed_time=10.0,
    )
    first = _make_node_execution(
        id="first-row-id",
        node_execution_id="first-exec-id",
        node_id="first-node",
        title="First",
        node_type="template-transform",
        process_data="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 3),
        elapsed_time=2.0,
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    second = _make_node_execution(
        id="second-row-id",
        node_execution_id="second-exec-id",
        node_id="second-node",
        title="Second",
        node_type="template-transform",
        status="failed",
        error="boom",
        process_data="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 4),
        elapsed_time=5.0,
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, first, second])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    wrapper = next(span for span in tracer.spans if span.name == "loop[0]")
    assert wrapper.start_time == datetime_to_nanos(first.created_at)
    assert wrapper.end_time == datetime_to_nanos(second.created_at + timedelta(seconds=second.elapsed_time))
    assert wrapper.status.status_code == StatusCode.ERROR


def test_workflow_trace_keeps_loop_body_nodes_under_loop_without_index(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    template = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node"}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, template])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "loop_main_loop",
        "template-transform_Template",
    ]
    assert tracer.spans[3].parent_name == "loop_main_loop"


def test_workflow_trace_ignores_malformed_loop_index(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    template = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": " 1"}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, template])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "loop_main_loop",
        "template-transform_Template",
    ]
    assert tracer.spans[3].parent_name == "loop_main_loop"


def test_workflow_trace_skips_wrapper_when_container_graph_id_is_ambiguous(monkeypatch):
    first_loop = _make_node_execution(
        id="loop-row-id-1",
        node_execution_id="loop-exec-id-1",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    second_loop = _make_node_execution(
        id="loop-row-id-2",
        node_execution_id="loop-exec-id-2",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    child = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[first_loop, second_loop, child])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert "loop[0]" not in [span.name for span in tracer.spans]
    child_span = next(span for span in tracer.spans if span.name == "template-transform_Template")
    assert child_span.parent_name == "Root_Chat_workflow"


def test_workflow_trace_ends_loop_wrapper_when_child_export_raises(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    tool = _make_node_execution(
        id="tool-row-id",
        node_execution_id="tool-exec-id",
        node_id="tool-node",
        title="Embedded Workflow",
        node_type="tool",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, tool])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.TraceContextTextMapPropagator",
        lambda: SimpleNamespace(inject=lambda carrier, context=None: carrier.update({"traceparent": "fake"})),
    )

    def raise_publish_error(node_execution, carrier):
        raise RuntimeError("publish failed")

    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._publish_parent_span_context_aliases",
        raise_publish_error,
    )

    with pytest.raises(RuntimeError, match="publish failed"):
        instance.workflow_trace(_make_workflow_trace_info())

    wrapper_span = next(span for span in tracer.spans if span.name == "loop[0]")
    assert wrapper_span.ended is True
    assert wrapper_span.end_count == 1
