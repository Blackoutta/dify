import logging
import sys
from unittest.mock import MagicMock

import pytest

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.entities import WorkflowLogEvent, WorkflowLogEventType


class FakeConnection:
    def __init__(self, hosts, timeout=None):
        self.hosts = hosts
        self.timeout = timeout
        self.connected = False
        self.sent = []
        self.disconnected = False

    def connect(self, username=None, passcode=None, wait=True):
        self.connected = True
        self.username = username
        self.passcode = passcode
        self.wait = wait

    def send(self, destination, body, headers=None):
        self.sent.append({"destination": destination, "body": body, "headers": headers or {}})

    def disconnect(self):
        self.disconnected = True
        self.connected = False


def test_activemq_publisher_sends_json_with_group_header(monkeypatch):
    fake_module = MagicMock()
    fake_module.Connection = FakeConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username="user",
        password="pass",
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    publisher.publish(event)

    connection = publisher._connection
    assert connection.hosts == [("mq.local", 61613)]
    assert connection.username == "user"
    assert connection.sent[0]["destination"] == "/queue/dify.workflow.logs"
    assert '"event_type":"workflow_node_execution.upsert"' in connection.sent[0]["body"]
    assert connection.sent[0]["headers"]["JMSXGroupID"] == "run-1"
    assert connection.sent[0]["headers"]["content_type"] == "application/json"


def test_activemq_publisher_resets_connection_on_send_failure(monkeypatch):
    class FailingConnection(FakeConnection):
        def send(self, destination, body, headers=None):
            raise RuntimeError("broker down")

    fake_module = MagicMock()
    fake_module.Connection = FailingConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username=None,
        password=None,
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
        max_retries=0,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    try:
        publisher.publish(event)
    except RuntimeError:
        pass

    assert publisher._connection is None


def test_activemq_publisher_retries_send_failure_with_new_connection(monkeypatch):
    created_connections = []

    class FailsOnceConnection(FakeConnection):
        def __init__(self, hosts, timeout=None):
            super().__init__(hosts, timeout)
            created_connections.append(self)

        def send(self, destination, body, headers=None):
            if len(created_connections) == 1:
                raise RuntimeError("stale connection")
            super().send(destination, body, headers)

    fake_module = MagicMock()
    fake_module.Connection = FailsOnceConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username=None,
        password=None,
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
        max_retries=1,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    publisher.publish(event)

    assert len(created_connections) == 2
    assert created_connections[0].disconnected is True
    assert len(created_connections[1].sent) == 1
    assert publisher._connection is created_connections[1]


def test_activemq_publisher_retries_connect_failure(monkeypatch):
    created_connections = []

    class ConnectFailsOnceConnection(FakeConnection):
        def __init__(self, hosts, timeout=None):
            super().__init__(hosts, timeout)
            created_connections.append(self)

        def connect(self, username=None, passcode=None, wait=True):
            if len(created_connections) == 1:
                raise RuntimeError("connect failed")
            super().connect(username=username, passcode=passcode, wait=wait)

    fake_module = MagicMock()
    fake_module.Connection = ConnectFailsOnceConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username="user",
        password="pass",
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
        max_retries=1,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    publisher.publish(event)

    assert len(created_connections) == 2
    assert created_connections[1].username == "user"
    assert len(created_connections[1].sent) == 1


def test_activemq_publisher_exhausts_retries_and_clears_connection(monkeypatch):
    class AlwaysFailingConnection(FakeConnection):
        def send(self, destination, body, headers=None):
            raise RuntimeError("broker down")

    fake_module = MagicMock()
    fake_module.Connection = AlwaysFailingConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username=None,
        password=None,
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
        max_retries=1,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    with pytest.raises(RuntimeError, match="broker down"):
        publisher.publish(event)

    assert publisher._connection is None


def test_activemq_publisher_logs_slow_publish_timing(monkeypatch, caplog):
    fake_module = MagicMock()
    fake_module.Connection = FakeConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    timings = iter([10.0, 10.2, 10.7])
    monkeypatch.setattr("core.workflow.log_publisher.activemq_publisher.time.perf_counter", lambda: next(timings))

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username=None,
        password=None,
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
        max_retries=1,
        slow_log_threshold=0.1,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    with caplog.at_level(logging.WARNING, logger="core.workflow.log_publisher.activemq_publisher"):
        publisher.publish(event)

    record = next(record for record in caplog.records if "Slow ActiveMQ workflow log publish" in record.message)
    assert "workflow_run_id=run-1" in record.message
    assert "total_ms=700.000" in record.message
    assert "lock_wait_ms=200.000" in record.message
    assert "send_ms=500.000" in record.message
    assert "attempts=1" in record.message
    assert "success=True" in record.message
    assert record.workflow_run_id == "run-1"
    assert record.destination == "/queue/dify.workflow.logs"
    assert record.success is True
    assert record.attempts == 1
    assert record.total_ms == pytest.approx(700.0)
    assert record.lock_wait_ms == pytest.approx(200.0)
    assert record.send_ms == pytest.approx(500.0)


def test_activemq_publisher_close_disconnects_cached_connection(monkeypatch):
    fake_module = MagicMock()
    fake_module.Connection = FakeConnection
    monkeypatch.setitem(sys.modules, "stomp", fake_module)

    publisher = ActiveMQWorkflowLogPublisher(
        host="mq.local",
        port=61613,
        username=None,
        password=None,
        destination="/queue/dify.workflow.logs",
        timeout=0.2,
        max_retries=1,
    )
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "id": "node-1"},
    )

    publisher.publish(event)
    connection = publisher._connection

    publisher.close()
    publisher.close()

    assert connection.disconnected is True
    assert publisher._connection is None
