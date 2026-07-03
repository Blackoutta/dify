"""ActiveMQ publisher repository for workflow node execution snapshots.

This repository is producer-only. The API process keeps an in-memory read cache
for workflow runtime lookups, while the external consumer owns durable database
writes and truncation.
"""

import json
import logging
import socket
import uuid
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from threading import Lock
from typing import Any

from configs import dify_config
from dify_graph.entities import WorkflowNodeExecution
from dify_graph.model_runtime.utils.encoders import jsonable_encoder
from dify_graph.repositories.workflow_node_execution_repository import OrderConfig, WorkflowNodeExecutionRepository
from dify_graph.workflow_type_encoder import WorkflowRuntimeTypeConverter
from libs.helper import extract_tenant_id
from models import Account, CreatorUserRole, EndUser, WorkflowNodeExecutionTriggeredFrom

logger = logging.getLogger(__name__)


def _value(value: Any) -> Any:
    return value.value if hasattr(value, "value") else value


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC).isoformat().replace("+00:00", "Z")


def _frame(command: str, headers: dict[str, str], body: bytes = b"") -> bytes:
    header_lines = "\n".join(f"{key}:{value}" for key, value in headers.items())
    return f"{command}\n{header_lines}\n\n".encode() + body + b"\x00"


class WorkflowNodeExecutionActiveMQPublisher:
    """Minimal STOMP 1.2 publisher using the stdlib socket module."""

    def publish(self, event: dict[str, Any]) -> None:
        body = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        headers = {
            "destination": dify_config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
            "content-type": "application/json",
            "event_type": event["event_type"],
            "schema_version": str(event["schema_version"]),
            "JMSXGroupID": event["payload"]["workflow_run_id"] or event["payload"]["id"],
            "content-length": str(len(body)),
        }
        for attempt in range(dify_config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES + 1):
            try:
                self._send(body, headers)
                return
            except OSError:
                if attempt >= dify_config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES:
                    raise

    def _send(self, body: bytes, headers: dict[str, str]) -> None:
        timeout = dify_config.WORKFLOW_LOG_PUBLISH_TIMEOUT
        with socket.create_connection(
            (dify_config.WORKFLOW_LOG_ACTIVEMQ_HOST, dify_config.WORKFLOW_LOG_ACTIVEMQ_PORT),
            timeout=timeout,
        ) as sock:
            sock.settimeout(timeout)
            connect_headers = {
                "accept-version": "1.2",
                "host": dify_config.WORKFLOW_LOG_ACTIVEMQ_HOST,
            }
            if dify_config.WORKFLOW_LOG_ACTIVEMQ_USERNAME:
                connect_headers["login"] = dify_config.WORKFLOW_LOG_ACTIVEMQ_USERNAME
                connect_headers["passcode"] = dify_config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD
            sock.sendall(_frame("CONNECT", connect_headers))
            sock.recv(1024)
            sock.sendall(_frame("SEND", headers, body))
            sock.sendall(_frame("DISCONNECT", {}))


class ActiveMQWorkflowNodeExecutionRepository(WorkflowNodeExecutionRepository):
    """Publishes workflow node execution snapshots and keeps an in-memory read cache."""

    _tenant_id: str
    _app_id: str | None
    _triggered_from: WorkflowNodeExecutionTriggeredFrom
    _creator_user_id: str
    _creator_user_role: CreatorUserRole
    _publish_event: Callable[[dict[str, Any]], None]
    _execution_cache: dict[str, WorkflowNodeExecution]
    _workflow_run_mapping: dict[str, list[str]]
    _state_versions: dict[str, int]
    _lock: Any

    def __init__(
        self,
        *,
        user: Account | EndUser,
        app_id: str | None,
        triggered_from: WorkflowNodeExecutionTriggeredFrom,
        publisher: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        tenant_id = extract_tenant_id(user)
        if not tenant_id:
            raise ValueError("User must have a tenant_id or current_tenant_id")

        self._tenant_id = tenant_id
        self._app_id = app_id
        self._triggered_from = triggered_from
        self._creator_user_id = user.id
        self._creator_user_role = CreatorUserRole.ACCOUNT if isinstance(user, Account) else CreatorUserRole.END_USER
        self._publish_event = publisher or WorkflowNodeExecutionActiveMQPublisher().publish
        self._execution_cache = {}
        self._workflow_run_mapping = {}
        self._state_versions = {}
        self._lock = Lock()

    def save(self, execution: WorkflowNodeExecution) -> None:
        with self._lock:
            self._cache_execution(execution)
            state_version = self._state_versions.get(execution.id, 0) + 1
            self._state_versions[execution.id] = state_version
            event = self._build_event(execution, state_version)

        try:
            self._publish_event(event)
        except Exception:
            logger.exception(
                "Failed to publish workflow node execution log, "
                "tenant_id=%s app_id=%s workflow_run_id=%s node_execution_id=%s",
                self._tenant_id,
                self._app_id,
                execution.workflow_execution_id,
                execution.node_execution_id,
            )

    def save_execution_data(self, execution: WorkflowNodeExecution) -> None:
        return

    def get_by_workflow_run(
        self,
        workflow_run_id: str,
        order_config: OrderConfig | None = None,
    ) -> Sequence[WorkflowNodeExecution]:
        execution_ids = self._workflow_run_mapping.get(workflow_run_id, [])
        result = [
            self._execution_cache[execution_id]
            for execution_id in execution_ids
            if execution_id in self._execution_cache
        ]
        if order_config:
            reverse = order_config.order_direction == "desc"
            for field_name in reversed(order_config.order_by):
                result.sort(key=lambda execution: getattr(execution, field_name, 0), reverse=reverse)
        return result

    def _cache_execution(self, execution: WorkflowNodeExecution) -> None:
        self._execution_cache[execution.id] = execution
        if not execution.workflow_execution_id:
            return
        execution_ids = self._workflow_run_mapping.setdefault(execution.workflow_execution_id, [])
        if execution.id not in execution_ids:
            execution_ids.append(execution.id)

    def _build_event(self, execution: WorkflowNodeExecution, state_version: int) -> dict[str, Any]:
        converter = WorkflowRuntimeTypeConverter()
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": "workflow_node_execution.upsert",
            "schema_version": 1,
            "created_at": _iso_utc(datetime.now(UTC)),
            "payload": {
                "id": execution.id,
                "tenant_id": self._tenant_id,
                "app_id": self._app_id,
                "workflow_id": execution.workflow_id,
                "workflow_run_id": execution.workflow_execution_id,
                "node_execution_id": execution.node_execution_id,
                "node_id": execution.node_id,
                "node_type": _value(execution.node_type),
                "title": execution.title,
                "triggered_from": self._triggered_from.value,
                "index": execution.index,
                "predecessor_node_id": execution.predecessor_node_id,
                "inputs": converter.to_json_encodable(execution.inputs),
                "process_data": converter.to_json_encodable(execution.process_data),
                "outputs": converter.to_json_encodable(execution.outputs),
                "status": execution.status.value,
                "error": execution.error,
                "elapsed_time": execution.elapsed_time,
                "execution_metadata": jsonable_encoder(execution.metadata or {}),
                "created_by_role": self._creator_user_role.value,
                "created_by": self._creator_user_id,
                "created_at": _iso_utc(execution.created_at),
                "finished_at": _iso_utc(execution.finished_at),
                "state_version": state_version,
            },
        }
