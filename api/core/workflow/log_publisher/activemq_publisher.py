from __future__ import annotations

import logging
import threading
import time
from typing import Any

from core.workflow.log_publisher.entities import WorkflowLogEvent

logger = logging.getLogger(__name__)


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
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._destination = destination
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._slow_log_threshold = max(0.0, slow_log_threshold)
        self._connection: Any | None = None
        self._lock = threading.RLock()

    def publish(self, event: WorkflowLogEvent) -> None:
        headers = self._headers_for(event)
        body = event.model_dump_json()
        publish_started_at = time.perf_counter()
        lock_wait_seconds = 0.0
        attempts = 0
        success = False

        try:
            with self._lock:
                lock_wait_seconds = time.perf_counter() - publish_started_at
                for attempt in range(self._max_retries + 1):
                    attempts = attempt + 1
                    try:
                        connection = self._ensure_connection()
                        connection.send(
                            destination=self._destination,
                            body=body,
                            headers=headers,
                        )
                        success = True
                        return
                    except Exception:
                        self._reset_connection()
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
            )

    def close(self) -> None:
        with self._lock:
            self._reset_connection()

    def _log_slow_publish(
        self,
        *,
        event: WorkflowLogEvent,
        total_seconds: float,
        lock_wait_seconds: float,
        send_seconds: float,
        attempts: int,
        success: bool,
    ) -> None:
        if total_seconds < self._slow_log_threshold:
            return
        workflow_run_id = event.payload.get("workflow_run_id")
        node_execution_id = event.payload.get("node_execution_id") or event.payload.get("id")
        logger.warning(
            "Slow ActiveMQ workflow log publish "
            "event_id=%s workflow_run_id=%s node_execution_id=%s destination=%s "
            "total_ms=%.3f lock_wait_ms=%.3f send_ms=%.3f attempts=%s success=%s",
            event.event_id,
            workflow_run_id,
            node_execution_id,
            self._destination,
            total_seconds * 1000,
            lock_wait_seconds * 1000,
            send_seconds * 1000,
            attempts,
            success,
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

    def _ensure_connection(self):
        if self._connection is not None:
            is_connected = getattr(self._connection, "is_connected", None)
            if not callable(is_connected) or is_connected():
                return self._connection
            self._reset_connection()

        try:
            import stomp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("stomp.py is required when async workflow log publishing is enabled") from exc

        connection = stomp.Connection([(self._host, self._port)], timeout=self._timeout)
        connection.connect(username=self._username, passcode=self._password, wait=True)
        self._connection = connection
        return connection

    def _reset_connection(self) -> None:
        connection = self._connection
        self._connection = None
        if connection is None:
            return
        try:
            connection.disconnect()
        except Exception:
            pass
