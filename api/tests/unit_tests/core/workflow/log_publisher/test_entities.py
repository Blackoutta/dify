from datetime import datetime
from decimal import Decimal

from core.workflow.log_publisher.entities import (
    NodeExecutionTraceSnapshot,
    WorkflowLogEvent,
    WorkflowLogEventType,
    WorkflowLogWriteMode,
    dump_json_safe,
)


def test_dump_json_safe_serializes_datetime_enum_and_decimal():
    payload = dump_json_safe(
        {
            "created_at": datetime(2026, 6, 6, 1, 2, 3),
            "mode": WorkflowLogWriteMode.ASYNC,
            "price": Decimal("1.25"),
        }
    )

    assert payload == {
        "created_at": "2026-06-06T01:02:03Z",
        "mode": "async",
        "price": 1.25,
    }


def test_node_execution_event_is_json_serializable():
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "created_at": datetime(2026, 6, 6, 1, 2, 3)},
    )

    dumped = event.model_dump(mode="json")

    assert dumped["event_type"] == "workflow_node_execution.upsert"
    assert dumped["schema_version"] == 1
    assert dumped["payload"] == {"workflow_run_id": "run-1", "created_at": "2026-06-06T01:02:03Z"}
    assert dumped["event_id"]
    assert dumped["created_at"].endswith("Z")


def test_node_trace_snapshot_is_json_safe():
    snapshot = NodeExecutionTraceSnapshot(
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

    dumped = snapshot.model_dump(mode="json")

    assert dumped["created_at"] == "2026-06-06T01:02:03Z"
    assert dumped["finished_at"] == "2026-06-06T01:02:04Z"
