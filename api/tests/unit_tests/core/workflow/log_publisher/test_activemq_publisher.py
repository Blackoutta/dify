import sys
from unittest.mock import MagicMock

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
