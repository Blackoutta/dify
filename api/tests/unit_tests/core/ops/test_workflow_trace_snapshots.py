from datetime import datetime

from core.ops.entities.trace_entity import WorkflowTraceInfo
from core.workflow.log_publisher.entities import NodeExecutionTraceSnapshot, WorkflowRunTraceSnapshot


def test_trace_snapshots_are_json_safe_through_workflow_trace_info():
    workflow_snapshot = WorkflowRunTraceSnapshot(
        id="run-1",
        tenant_id="tenant-1",
        app_id="app-1",
        workflow_id="workflow-1",
        triggered_from="app-run",
        type="workflow",
        version="1",
        graph={},
        inputs={"query": "hello"},
        outputs={"answer": "world"},
        status="succeeded",
        error=None,
        elapsed_time=1.2,
        total_tokens=10,
        total_steps=2,
        exceptions_count=0,
        created_at=datetime(2026, 6, 6, 1, 2, 3),
        finished_at=datetime(2026, 6, 6, 1, 2, 4),
    )
    node_snapshot = NodeExecutionTraceSnapshot(
        id="record-1",
        workflow_run_id="run-1",
        node_execution_id="node-exec-1",
        node_id="llm",
        node_type="llm",
        title="LLM",
        inputs={"query": "hello"},
        process_data={"prompts": []},
        outputs={"text": "world"},
        status="succeeded",
        error=None,
        elapsed_time=1.0,
        metadata={"total_tokens": 10},
        created_at=datetime(2026, 6, 6, 1, 2, 3),
        finished_at=datetime(2026, 6, 6, 1, 2, 4),
    )

    trace_info = WorkflowTraceInfo(
        workflow_data={},
        conversation_id=None,
        workflow_id="workflow-1",
        tenant_id="tenant-1",
        workflow_run_id="run-1",
        workflow_run_elapsed_time=1.2,
        workflow_run_status="succeeded",
        workflow_run_inputs={"query": "hello"},
        workflow_run_outputs={"answer": "world"},
        workflow_run_version="1",
        error=None,
        total_tokens=10,
        file_list=[],
        query="hello",
        metadata={"app_id": "app-1"},
        workflow_snapshot=workflow_snapshot.model_dump(mode="json"),
        node_execution_snapshots=[node_snapshot.model_dump(mode="json")],
    )

    restored = WorkflowTraceInfo.model_validate_json(trace_info.model_dump_json())

    assert restored.workflow_snapshot["created_at"] == "2026-06-06T01:02:03Z"
    assert restored.node_execution_snapshots[0]["metadata"] == {"total_tokens": 10}
