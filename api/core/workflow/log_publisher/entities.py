from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class WorkflowLogWriteMode(StrEnum):
    SYNC = "sync"
    ASYNC = "async"


class WorkflowLogEventType(StrEnum):
    WORKFLOW_RUN_UPSERT = "workflow_run.upsert"
    WORKFLOW_NODE_EXECUTION_UPSERT = "workflow_node_execution.upsert"


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat() + "Z"
    return value.astimezone(UTC).replace(tzinfo=None).isoformat() + "Z"


def dump_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(dump_json_safe(k)): dump_json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [dump_json_safe(item) for item in value]
    return value


class WorkflowRunTraceSnapshot(BaseModel):
    id: str
    tenant_id: str
    app_id: str | None = None
    workflow_id: str
    triggered_from: str
    type: str
    version: str
    graph: dict[str, Any] | None = None
    inputs: dict[str, Any] | None = None
    outputs: dict[str, Any] | None = None
    status: str
    error: str | None = None
    elapsed_time: int | float
    total_tokens: int
    total_steps: int
    exceptions_count: int
    created_at: datetime
    finished_at: datetime | None = None

    @field_serializer("created_at", "finished_at")
    def serialize_datetime_fields(self, value: datetime | None) -> str | None:
        return _serialize_datetime(value) if value else None


class NodeExecutionTraceSnapshot(BaseModel):
    id: str
    workflow_run_id: str
    node_execution_id: str | None = None
    node_id: str
    node_type: str
    title: str
    inputs: dict[str, Any] | None = None
    process_data: dict[str, Any] | None = None
    outputs: dict[str, Any] | None = None
    status: str
    error: str | None = None
    elapsed_time: int | float | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime
    finished_at: datetime | None = None

    @field_serializer("created_at", "finished_at")
    def serialize_datetime_fields(self, value: datetime | None) -> str | None:
        return _serialize_datetime(value) if value else None


class WorkflowLogEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: WorkflowLogEventType
    schema_version: int = 1
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC).replace(tzinfo=None))
    payload: dict[str, Any]

    model_config = ConfigDict(use_enum_values=True)

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return _serialize_datetime(value)

    @classmethod
    def create(cls, *, event_type: WorkflowLogEventType, payload: dict[str, Any]) -> WorkflowLogEvent:
        return cls(event_type=event_type, payload=dump_json_safe(payload))
