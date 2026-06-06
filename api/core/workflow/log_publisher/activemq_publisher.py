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
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._destination = destination
        self._timeout = timeout
        self._connection: Any | None = None
        self._lock = threading.RLock()

    def publish(self, event: WorkflowLogEvent) -> None:
        with self._lock:
            connection = self._ensure_connection()
            headers = {
                "event_type": event.event_type.value if hasattr(event.event_type, "value") else str(event.event_type),
                "schema_version": str(event.schema_version),
                "content_type": "application/json",
            }
            group_id = event.payload.get("workflow_run_id") or event.payload.get("id")
            if group_id:
                headers["JMSXGroupID"] = str(group_id)
            try:
                connection.send(
                    destination=self._destination,
                    body=event.model_dump_json(),
                    headers=headers,
                )
            except Exception:
                self._reset_connection()
                raise

    def _ensure_connection(self):
        if self._connection is not None:
            return self._connection
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
