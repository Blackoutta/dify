import json
from datetime import datetime
from types import SimpleNamespace

import pytest
from openinference.semconv.trace import OpenInferenceMimeTypeValues, SpanAttributes
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from core.ops.arize_phoenix_trace.arize_phoenix_trace import (
    ArizePhoenixDataTrace,
    _build_parent_span_bridge_id,
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
        self.status = None

    def get_span_context(self):
        return trace.INVALID_SPAN_CONTEXT

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def set_status(self, status):
        self.status = status

    def end(self, end_time=None):
        self.end_time = end_time
        self.ended = True


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


def test_missing_parent_span_context_raises_retryable_error(monkeypatch):
    monkeypatch.setattr("core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get", lambda key: None)

    with pytest.raises(PendingTraceParentContextError):
        _resolve_published_parent_span_context("outer-run:tool-node")


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


def test_workflow_trace_keeps_repeated_loop_body_nodes_under_loop(monkeypatch):
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
                    execution_metadata=json.dumps({"loop_id": "loop-node"}),
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
                    execution_metadata=json.dumps({"loop_id": "loop-node"}),
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
                    execution_metadata=json.dumps({"loop_id": "loop-node"}),
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

    body_spans = tracer.spans[3:]
    assert len(body_spans) == 9
    assert {span.parent_name for span in body_spans} == {"loop_main_loop"}
