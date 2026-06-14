from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from core.workflow.entities.workflow_node_execution import WorkflowNodeExecutionMetadataKey, WorkflowNodeExecutionStatus
from core.workflow.nodes.enums import NodeType


def _coerce_status(value: Any) -> Any:
    try:
        return WorkflowNodeExecutionStatus(value)
    except Exception:
        return value


def _coerce_node_type(value: Any) -> Any:
    try:
        return NodeType(value)
    except Exception:
        return value


def _coerce_datetime(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        from datetime import datetime

        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return value


def _coerce_metadata(metadata: dict[str, Any]) -> dict[Any, Any]:
    coerced: dict[Any, Any] = {}
    for key, value in metadata.items():
        try:
            coerced[WorkflowNodeExecutionMetadataKey(key)] = value
        except Exception:
            coerced[key] = value
    return coerced


def workflow_node_snapshot_to_domain_like(snapshot: dict[str, Any]) -> SimpleNamespace:
    metadata = _coerce_metadata(snapshot.get("metadata") or {})
    return SimpleNamespace(
        id=snapshot.get("id"),
        tenant_id=snapshot.get("tenant_id"),
        app_id=snapshot.get("app_id"),
        predecessor_node_id=snapshot.get("predecessor_node_id"),
        index=snapshot.get("index"),
        workflow_execution_id=snapshot.get("workflow_run_id"),
        workflow_run_id=snapshot.get("workflow_run_id"),
        node_execution_id=snapshot.get("node_execution_id"),
        node_id=snapshot.get("node_id"),
        node_type=_coerce_node_type(snapshot.get("node_type")),
        title=snapshot.get("title"),
        inputs=snapshot.get("inputs") or {},
        process_data=snapshot.get("process_data") or {},
        outputs=snapshot.get("outputs") or {},
        status=_coerce_status(snapshot.get("status")),
        error=snapshot.get("error"),
        elapsed_time=snapshot.get("elapsed_time") or 0,
        metadata=metadata,
        execution_metadata=metadata,
        created_at=_coerce_datetime(snapshot.get("created_at")),
        finished_at=_coerce_datetime(snapshot.get("finished_at")),
    )


def workflow_node_executions_from_snapshots(trace_info) -> list[SimpleNamespace] | None:
    snapshots = getattr(trace_info, "node_execution_snapshots", None)
    if not snapshots:
        return None
    return [workflow_node_snapshot_to_domain_like(snapshot) for snapshot in snapshots]
