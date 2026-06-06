from datetime import datetime
from decimal import Decimal

from core.workflow.log_publisher.entities import (
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


def test_workflow_log_event_is_json_serializable():
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_RUN_UPSERT,
        payload={"id": "run-1", "created_at": datetime(2026, 6, 6, 1, 2, 3)},
    )

    dumped = event.model_dump(mode="json")

    assert dumped["event_type"] == "workflow_run.upsert"
    assert dumped["schema_version"] == 1
    assert dumped["payload"] == {"id": "run-1", "created_at": "2026-06-06T01:02:03Z"}
    assert dumped["event_id"]
    assert dumped["created_at"].endswith("Z")
