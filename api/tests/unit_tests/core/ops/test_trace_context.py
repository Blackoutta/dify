from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from core.ops.entities.trace_entity import BaseTraceInfo, TraceTaskName
from core.ops.ops_trace_manager import TraceTask
from core.ops.trace_context import (
    ParentTraceContext,
    extract_parent_trace_context_from_args,
    extract_trace_session_id_from_args,
    normalize_trace_session_id,
    parent_trace_context_from_metadata,
)


def test_extract_parent_trace_context_from_args_accepts_complete_mapping():
    result = extract_parent_trace_context_from_args(
        {
            "parent_trace_context": {
                "parent_workflow_run_id": "outer-run",
                "parent_node_execution_id": "outer-run:tool-node",
            },
            "inputs": {"parent_trace_context": "user-input-must-stay-input"},
        }
    )

    assert result == {
        "parent_trace_context": ParentTraceContext(
            parent_workflow_run_id="outer-run",
            parent_node_execution_id="outer-run:tool-node",
        )
    }


def test_extract_parent_trace_context_from_args_rejects_incomplete_mapping():
    assert extract_parent_trace_context_from_args(
        {"parent_trace_context": {"parent_workflow_run_id": "outer-run"}}
    ) == {}


def test_normalize_trace_session_id_accepts_trimmed_string():
    assert normalize_trace_session_id("  external-session  ") == "external-session"


def test_normalize_trace_session_id_ignores_blank_or_none():
    assert normalize_trace_session_id(None) is None
    assert normalize_trace_session_id("   ") is None


def test_normalize_trace_session_id_rejects_non_string_and_long_values():
    with pytest.raises(ValueError):
        normalize_trace_session_id(123)
    with pytest.raises(ValueError):
        normalize_trace_session_id("x" * 513)


def test_extract_trace_session_id_from_args():
    assert extract_trace_session_id_from_args({"trace_session_id": " abc "}) == {"trace_session_id": "abc"}
    assert extract_trace_session_id_from_args({}) == {}


def test_parent_trace_context_from_metadata_accepts_model_and_mapping():
    model_context = ParentTraceContext(
        parent_workflow_run_id="outer-run",
        parent_node_execution_id="outer-run:tool-node",
    )

    assert parent_trace_context_from_metadata({"parent_trace_context": model_context}) == model_context
    assert parent_trace_context_from_metadata(
        {
            "parent_trace_context": {
                "parent_workflow_run_id": "outer-run",
                "parent_node_execution_id": "outer-run:tool-node",
            }
        }
    ) == model_context


def test_base_trace_info_resolved_parent_context_uses_private_metadata():
    trace_info = BaseTraceInfo(
        metadata={
            "parent_trace_context": {
                "parent_workflow_run_id": "outer-run",
                "parent_node_execution_id": "outer-run:tool-node",
            }
        }
    )

    assert trace_info.resolved_parent_context == ("outer-run", "outer-run:tool-node")


def test_trace_task_workflow_trace_keeps_parent_trace_context(monkeypatch):
    workflow_run = SimpleNamespace(
        id="child-run",
        workflow_id="workflow-id",
        tenant_id="tenant-id",
        elapsed_time=1.0,
        status="succeeded",
        inputs_dict={},
        outputs_dict={},
        version="1",
        error=None,
        total_tokens=0,
        app_id="app-id",
        triggered_from="workflow-run",
        created_at=None,
        finished_at=None,
        to_dict=lambda: {"id": "child-run"},
    )

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def scalars(self, stmt):
            return SimpleNamespace(first=lambda: workflow_run)

        def scalar(self, stmt):
            return None

    monkeypatch.setattr("core.ops.ops_trace_manager.Session", lambda *args, **kwargs: FakeSession())
    monkeypatch.setattr("core.ops.ops_trace_manager.db", SimpleNamespace(engine=object()))

    trace_task = TraceTask(
        TraceTaskName.WORKFLOW_TRACE,
        workflow_execution=SimpleNamespace(id_="child-run"),
        conversation_id=None,
        user_id="user-id",
        parent_trace_context=ParentTraceContext(
            parent_workflow_run_id="outer-run",
            parent_node_execution_id="outer-run:tool-node",
        ),
    )

    trace_info = trace_task.execute()

    assert trace_info.metadata["parent_trace_context"] == {
        "parent_workflow_run_id": "outer-run",
        "parent_node_execution_id": "outer-run:tool-node",
    }


def test_trace_task_workflow_trace_includes_trace_session_id(monkeypatch):
    workflow_run = SimpleNamespace(
        id="workflow-run",
        workflow_id="workflow-id",
        tenant_id="tenant-id",
        elapsed_time=1.0,
        status="succeeded",
        inputs_dict={},
        outputs_dict={},
        version="1",
        error=None,
        total_tokens=0,
        app_id="app-id",
        triggered_from="workflow-run",
        created_at=None,
        finished_at=None,
        to_dict=lambda: {"id": "workflow-run"},
    )

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def scalars(self, stmt):
            return SimpleNamespace(first=lambda: workflow_run)

        def scalar(self, stmt):
            return None

    monkeypatch.setattr("core.ops.ops_trace_manager.Session", lambda *args, **kwargs: FakeSession())
    monkeypatch.setattr("core.ops.ops_trace_manager.db", SimpleNamespace(engine=object()))

    trace_task = TraceTask(
        TraceTaskName.WORKFLOW_TRACE,
        workflow_execution=SimpleNamespace(id_="workflow-run"),
        conversation_id=None,
        user_id="user-id",
        trace_session_id="external-session",
    )

    trace_info = trace_task.execute()

    assert trace_info.metadata["trace_session_id"] == "external-session"


def test_trace_task_workflow_trace_includes_app_and_workspace_names(monkeypatch):
    workflow_run = SimpleNamespace(
        id="workflow-run",
        workflow_id="workflow-id",
        tenant_id="tenant-id",
        elapsed_time=1.0,
        status="succeeded",
        inputs_dict={},
        outputs_dict={},
        version="1",
        error=None,
        total_tokens=0,
        app_id="app-id",
        triggered_from="workflow-run",
        created_at=None,
        finished_at=None,
        to_dict=lambda: {"id": "workflow-run"},
    )

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def scalars(self, stmt):
            return SimpleNamespace(first=lambda: workflow_run)

        def scalar(self, stmt):
            return None

    monkeypatch.setattr("core.ops.ops_trace_manager.Session", lambda *args, **kwargs: FakeSession())
    monkeypatch.setattr("core.ops.ops_trace_manager.db", SimpleNamespace(engine=object()))
    monkeypatch.setattr(
        "core.ops.ops_trace_manager._lookup_app_and_workspace_names",
        lambda app_id, tenant_id: ("Root Chat", "Workspace"),
    )

    trace_task = TraceTask(
        TraceTaskName.WORKFLOW_TRACE,
        workflow_execution=SimpleNamespace(id_="workflow-run"),
        conversation_id=None,
        user_id="user-id",
    )

    trace_info = trace_task.execute()

    assert trace_info.metadata["app_name"] == "Root Chat"
    assert trace_info.metadata["workspace_name"] == "Workspace"


def test_trace_task_message_trace_includes_trace_session_id(monkeypatch):
    created_at = datetime.now(UTC).replace(tzinfo=None)
    message_data = SimpleNamespace(
        conversation_id="conversation-id",
        created_at=created_at,
        message="hello",
        model_provider="openai",
        model_id="gpt-4",
        status="normal",
        from_end_user_id="end-user-id",
        from_account_id=None,
        agent_based=False,
        workflow_run_id=None,
        from_source="api",
        message_tokens=3,
        answer_tokens=5,
        error=None,
        answer="hi",
        provider_response_latency=0,
        to_dict=lambda: {"id": "message-id"},
    )

    class FakeMessageFileQuery:
        def filter_by(self, **kwargs):
            return self

        def first(self):
            return None

    class FakeSession:
        def scalars(self, stmt):
            return SimpleNamespace(all=lambda: ["chat"])

        def query(self, model):
            return FakeMessageFileQuery()

    monkeypatch.setattr("core.ops.ops_trace_manager.get_message_data", lambda message_id: message_data)
    monkeypatch.setattr("core.ops.ops_trace_manager.db", SimpleNamespace(session=FakeSession()))

    trace_task = TraceTask(
        TraceTaskName.MESSAGE_TRACE,
        message_id="message-id",
        conversation_id="conversation-id",
        trace_session_id="external-session",
    )

    trace_info = trace_task.execute()

    assert trace_info.metadata["trace_session_id"] == "external-session"
