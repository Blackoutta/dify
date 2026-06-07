from __future__ import annotations

import threading
from typing import Any

from core.workflow.log_publisher.entities import WorkflowLogEvent


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
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._destination = destination
        self._timeout = timeout
        self._max_retries = max(0, max_retries)
        self._connection: Any | None = None
        self._lock = threading.RLock()

    def publish(self, event: WorkflowLogEvent) -> None:
        headers = self._headers_for(event)
        body = event.model_dump_json()

        with self._lock:
            for attempt in range(self._max_retries + 1):
                try:
                    connection = self._ensure_connection()
                    connection.send(
                        destination=self._destination,
                        body=body,
                        headers=headers,
                    )
                    return
                except Exception:
                    self._reset_connection()
                    if attempt >= self._max_retries:
                        raise

    def close(self) -> None:
        with self._lock:
            self._reset_connection()

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
