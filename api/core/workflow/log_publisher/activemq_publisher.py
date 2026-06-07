from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from core.workflow.log_publisher.entities import WorkflowLogEvent

logger = logging.getLogger(__name__)


@dataclass
class _ConnectionSlot:
    index: int
    lock: threading.RLock = field(default_factory=threading.RLock)
    connection: Any | None = None


class ActiveMQWorkflowLogPublisher:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        username: str | None,
        password: str | None,
        destination: str,
        timeout: float,
        max_retries: int = 1,
        slow_log_threshold: float = 0.5,
        pool_size: int = 1,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._destination = destination
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._slow_log_threshold = max(0.0, slow_log_threshold)
        self._pool_size = max(1, pool_size)
        self._slots = [_ConnectionSlot(index=index) for index in range(self._pool_size)]
        self._slot_selection_lock = threading.RLock()
        self._next_slot_index = 0
        # Backward-compatible test/debug handle for the first slot connection.
        self._connection: Any | None = None

    def publish(self, event: WorkflowLogEvent) -> None:
        headers = self._headers_for(event)
        body = event.model_dump_json()
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
                        connection.send(
                            destination=self._destination,
                            body=body,
                            headers=headers,
                        )
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

    def _log_slow_publish(
        self,
        *,
        event: WorkflowLogEvent,
        total_seconds: float,
        lock_wait_seconds: float,
        send_seconds: float,
        attempts: int,
        success: bool,
        pool_slot: int,
    ) -> None:
        if total_seconds < self._slow_log_threshold:
            return
        workflow_run_id = event.payload.get("workflow_run_id")
        node_execution_id = event.payload.get("node_execution_id") or event.payload.get("id")
        logger.warning(
            "Slow ActiveMQ workflow log publish "
            "event_id=%s workflow_run_id=%s node_execution_id=%s destination=%s "
            "total_ms=%.3f lock_wait_ms=%.3f send_ms=%.3f attempts=%s success=%s pool_slot=%s pool_size=%s",
            event.event_id,
            workflow_run_id,
            node_execution_id,
            self._destination,
            total_seconds * 1000,
            lock_wait_seconds * 1000,
            send_seconds * 1000,
            attempts,
            success,
            pool_slot,
            self._pool_size,
            extra={
                "event_id": event.event_id,
                "workflow_run_id": workflow_run_id,
                "node_execution_id": node_execution_id,
                "destination": self._destination,
                "total_ms": total_seconds * 1000,
                "lock_wait_ms": lock_wait_seconds * 1000,
                "send_ms": send_seconds * 1000,
                "attempts": attempts,
                "success": success,
                "pool_slot": pool_slot,
                "pool_size": self._pool_size,
            },
        )

    def _headers_for(self, event: WorkflowLogEvent) -> dict[str, str]:
        headers = {
            "event_type": event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type),
            "schema_version": str(event.schema_version),
            "content_type": "application/json",
        }
        group_id = event.payload.get("workflow_run_id") or event.payload.get("id")
        if group_id:
            headers["JMSXGroupID"] = str(group_id)
        return headers

    def _select_slot(self) -> _ConnectionSlot:
        with self._slot_selection_lock:
            slot = self._slots[self._next_slot_index]
            self._next_slot_index = (self._next_slot_index + 1) % self._pool_size
            return slot

    def _ensure_connection(self, slot: _ConnectionSlot):
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
        connection.connect(username=self._username, passcode=self._password, wait=True)
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
            pass

    def _sync_legacy_connection_handle(self) -> None:
        self._connection = self._slots[0].connection
