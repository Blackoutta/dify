from datetime import datetime
from types import SimpleNamespace

from core.ops.entities.trace_entity import MessageTraceInfo, WorkflowTraceInfo
from core.ops.unified_trace.trace_builder import resolve_session_id


def make_workflow_trace_info(**overrides) -> WorkflowTraceInfo:
    values = {
        "workflow_data": SimpleNamespace(),
        "conversation_id": None,
        "workflow_id": "workflow-1",
        "tenant_id": "tenant-1",
        "workflow_run_id": "run-1",
        "workflow_run_elapsed_time": 1.0,
        "workflow_run_status": "succeeded",
        "workflow_run_inputs": {},
        "workflow_run_outputs": {},
        "workflow_run_version": "1",
        "total_tokens": 0,
        "file_list": [],
        "query": "",
        "metadata": {},
    }
    values.update(overrides)
    return WorkflowTraceInfo(**values)


def test_custom_session_id_wins_over_conversation_id():
    info = make_workflow_trace_info(
        conversation_id="conversation-1",
        metadata={"trace_session_id": "customer-session"},
    )

    assert resolve_session_id(info) == "customer-session"


def test_workflow_session_falls_back_to_conversation_then_run():
    assert resolve_session_id(make_workflow_trace_info(conversation_id="conversation-1")) == "conversation-1"
    assert resolve_session_id(make_workflow_trace_info()) == "run-1"


def test_nested_workflow_session_falls_back_to_parent_workflow():
    info = make_workflow_trace_info(
        metadata={
            "parent_trace_context": {
                "parent_workflow_run_id": "parent-run",
                "parent_node_execution_id": "parent-node-execution",
            }
        }
    )

    assert resolve_session_id(info) == "parent-run"


def test_message_session_falls_back_to_message_conversation():
    info = MessageTraceInfo(
        conversation_model="chat",
        message_tokens=0,
        answer_tokens=0,
        total_tokens=0,
        conversation_mode="chat",
        message_data=SimpleNamespace(conversation_id="conversation-1", created_at=datetime(2025, 1, 1)),
        metadata={},
    )

    assert resolve_session_id(info) == "conversation-1"
