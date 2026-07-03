"""ActiveMQ publisher repository for workflow node execution snapshots.

This repository is producer-only. The API process keeps an in-memory read cache
for workflow runtime lookups, while the external consumer owns durable database
writes and truncation.
"""

import atexit
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
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


@dataclass
class _ConnectionSlot:
    index: int
    lock: threading.RLock = field(default_factory=threading.RLock)
    connection: Any | None = None


@dataclass(frozen=True)
class _PublisherConfigKey:
    host: str
    port: int
    username: str
    password: str
    destination: str
    timeout: float
    max_retries: int
    slow_log_threshold: float
    pool_size: int


_publisher_lock = threading.RLock()
_publishers: dict[_PublisherConfigKey, "WorkflowNodeExecutionActiveMQPublisher"] = {}


class WorkflowNodeExecutionActiveMQPublisher:
    """Pooled ActiveMQ STOMP publisher for workflow node execution events."""

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        destination: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        slow_log_threshold: float | None = None,
        pool_size: int | None = None,
    ) -> None:
        self._host = host or dify_config.WORKFLOW_LOG_ACTIVEMQ_HOST
        self._port = port or dify_config.WORKFLOW_LOG_ACTIVEMQ_PORT
        self._username = username if username is not None else dify_config.WORKFLOW_LOG_ACTIVEMQ_USERNAME
        self._password = password if password is not None else dify_config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD
        self._destination = destination or dify_config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION
        self._timeout = timeout if timeout is not None else dify_config.WORKFLOW_LOG_PUBLISH_TIMEOUT
        retries = max_retries if max_retries is not None else dify_config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES
        threshold = (
            slow_log_threshold
            if slow_log_threshold is not None
            else dify_config.WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD
        )
        self._max_retries = max(0, retries)
        self._slow_log_threshold = max(0.0, threshold)
        self._pool_size = max(1, pool_size if pool_size is not None else dify_config.WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE)
        self._slots = [_ConnectionSlot(index=index) for index in range(self._pool_size)]
        self._slot_selection_lock = threading.RLock()
        self._next_slot_index = 0
        self._connection: Any | None = None

    def publish(self, event: dict[str, Any]) -> None:
        body = json.dumps(event, separators=(",", ":"), ensure_ascii=False)
        headers = {
            "destination": self._destination,
            "content-type": "application/json",
            "event_type": event["event_type"],
            "schema_version": str(event["schema_version"]),
            "JMSXGroupID": event["payload"]["workflow_run_id"] or event["payload"]["id"],
        }
        publish_started_at = time.perf_counter()
        lock_wait_seconds = 0.0
        attempts = 0
        success = False
        slot = self._select_slot()

        try:
            with slot.lock:
                lock_wait_seconds = time.perf_counter() - publish_started_at
                for attempt in range(self._max_retries + 1):
                    attempts = attempt + 1
                    try:
                        connection = self._ensure_connection(slot)
                        connection.send(destination=self._destination, body=body, headers=headers)
                        success = True
                        return
                    except Exception:
                        self._reset_connection(slot)
                        if attempt >= self._max_retries:
                            raise
        finally:
            total_seconds = time.perf_counter() - publish_started_at
            self._log_slow_publish(
                event=event,
                total_seconds=total_seconds,
                lock_wait_seconds=lock_wait_seconds,
                send_seconds=max(0.0, total_seconds - lock_wait_seconds),
                attempts=attempts,
                success=success,
                pool_slot=slot.index,
            )

    def warm_up(self) -> None:
        for slot in self._slots:
            with slot.lock:
                self._ensure_connection(slot)

    def close(self) -> None:
        for slot in self._slots:
            with slot.lock:
                self._reset_connection(slot)

    def _select_slot(self) -> _ConnectionSlot:
        with self._slot_selection_lock:
            slot = self._slots[self._next_slot_index]
            self._next_slot_index = (self._next_slot_index + 1) % self._pool_size
            return slot

    def _ensure_connection(self, slot: _ConnectionSlot) -> Any:
        if slot.connection is not None:
            is_connected = getattr(slot.connection, "is_connected", None)
            if not callable(is_connected) or is_connected():
                return slot.connection
            self._reset_connection(slot)

        try:
            import stomp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("stomp.py is required when async workflow log publishing is enabled") from exc

        connection = stomp.Connection([(self._host, self._port)], timeout=self._timeout)
        connection.connect(
            username=self._username or None,
            passcode=self._password or None,
            wait=True,
        )
        slot.connection = connection
        self._sync_legacy_connection_handle()
        return connection

    def _reset_connection(self, slot: _ConnectionSlot) -> None:
        connection = slot.connection
        slot.connection = None
        self._sync_legacy_connection_handle()
        if connection is None:
            return
        try:
            connection.disconnect()
        except Exception:
            logger.debug("Failed to disconnect ActiveMQ workflow log producer", exc_info=True)

    def _sync_legacy_connection_handle(self) -> None:
        self._connection = self._slots[0].connection

    def _log_slow_publish(
        self,
        *,
        event: dict[str, Any],
        total_seconds: float,
        lock_wait_seconds: float,
        send_seconds: float,
        attempts: int,
        success: bool,
        pool_slot: int,
    ) -> None:
        if total_seconds < self._slow_log_threshold:
            return
        payload = event.get("payload") or {}
        logger.warning(
            "Slow ActiveMQ workflow log publish "
            "event_id=%s workflow_run_id=%s node_execution_id=%s destination=%s "
            "total_ms=%.3f lock_wait_ms=%.3f send_ms=%.3f attempts=%s success=%s pool_slot=%s pool_size=%s",
            event.get("event_id"),
            payload.get("workflow_run_id"),
            payload.get("node_execution_id") or payload.get("id"),
            self._destination,
            total_seconds * 1000,
            lock_wait_seconds * 1000,
            send_seconds * 1000,
            attempts,
            success,
            pool_slot,
            self._pool_size,
        )


def get_workflow_node_execution_activemq_publisher() -> WorkflowNodeExecutionActiveMQPublisher:
    key = _PublisherConfigKey(
        host=dify_config.WORKFLOW_LOG_ACTIVEMQ_HOST,
        port=dify_config.WORKFLOW_LOG_ACTIVEMQ_PORT,
        username=dify_config.WORKFLOW_LOG_ACTIVEMQ_USERNAME,
        password=dify_config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD,
        destination=dify_config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
        timeout=dify_config.WORKFLOW_LOG_PUBLISH_TIMEOUT,
        max_retries=dify_config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES,
        slow_log_threshold=dify_config.WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD,
        pool_size=dify_config.WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE,
    )
    with _publisher_lock:
        publisher = _publishers.get(key)
        if publisher is not None:
            return publisher

        publisher = WorkflowNodeExecutionActiveMQPublisher(
            host=key.host,
            port=key.port,
            username=key.username,
            password=key.password,
            destination=key.destination,
            timeout=key.timeout,
            max_retries=key.max_retries,
            slow_log_threshold=key.slow_log_threshold,
            pool_size=key.pool_size,
        )
        _publishers[key] = publisher
        atexit.register(publisher.close)
        return publisher


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
        self._publish_event = publisher or get_workflow_node_execution_activemq_publisher().publish
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
