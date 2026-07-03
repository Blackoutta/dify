import logging
import sys
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from core.repositories.workflow_node_execution_activemq_repository import (
    ActiveMQWorkflowNodeExecutionRepository,
    WorkflowNodeExecutionActiveMQPublisher,
)
from dify_graph.entities import WorkflowNodeExecution
from dify_graph.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from models import Account, CreatorUserRole, Tenant, WorkflowNodeExecutionTriggeredFrom


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def publish(self, event: dict[str, Any]) -> None:
        self.messages.append(event)


class FakeStompConnection:
    def __init__(self, *, fail_send_once: bool = False) -> None:
        self.fail_send_once = fail_send_once
        self.connected = False
        self.sent: list[dict[str, Any]] = []
        self.disconnect_calls = 0

    def connect(self, username: str | None = None, passcode: str | None = None, wait: bool = True) -> None:
        self.connected = True

    def is_connected(self) -> bool:
        return self.connected

    def send(self, **kwargs: Any) -> None:
        if self.fail_send_once:
            self.fail_send_once = False
            raise OSError("send failed")
        self.sent.append(kwargs)

    def disconnect(self) -> None:
        self.disconnect_calls += 1
        self.connected = False


def _install_fake_stomp(monkeypatch, connections: list[FakeStompConnection]) -> None:
    def connection_factory(*args: Any, **kwargs: Any) -> FakeStompConnection:
        connection = connections[len(created_connections)]
        created_connections.append(connection)
        return connection

    created_connections: list[FakeStompConnection] = []
    monkeypatch.setitem(sys.modules, "stomp", SimpleNamespace(Connection=connection_factory))


def _event() -> dict[str, Any]:
    return {
        "event_id": "event-id",
        "event_type": "workflow_node_execution.upsert",
        "schema_version": 1,
        "created_at": "2026-07-03T00:00:00Z",
        "payload": {
            "id": "row-id",
            "workflow_run_id": "run-id",
            "state_version": 1,
        },
    }


def _account() -> Account:
    account = Account(name="Test", email="test@example.com")
    account.id = "user-id"
    tenant = Tenant(name="Tenant")
    tenant.id = "tenant-id"
    account._current_tenant = tenant
    return account


def _execution(status: WorkflowNodeExecutionStatus = WorkflowNodeExecutionStatus.RUNNING) -> WorkflowNodeExecution:
    return WorkflowNodeExecution(
        id="row-id",
        node_execution_id="node-exec-id",
        workflow_id="workflow-id",
        workflow_execution_id="run-id",
        index=1,
        predecessor_node_id="start",
        node_id="llm",
        node_type=BuiltinNodeTypes.LLM,
        title="LLM",
        inputs={"prompt": "hello"},
        process_data={},
        outputs={"answer": "world"},
        status=status,
        error=None,
        elapsed_time=1.2,
        metadata={},
        created_at=datetime(2026, 7, 2, 0, 0, 0),
        finished_at=datetime(2026, 7, 2, 0, 0, 1),
    )


def _repo(publisher) -> ActiveMQWorkflowNodeExecutionRepository:
    return ActiveMQWorkflowNodeExecutionRepository(
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        publisher=publisher,
    )


def test_repository_publishes_full_snapshot_and_increments_state_version() -> None:
    publisher = FakePublisher()
    repo = _repo(publisher.publish)

    execution = _execution()
    repo.save(execution)
    repo.save(execution)

    assert [event["payload"]["state_version"] for event in publisher.messages] == [1, 2]
    payload = publisher.messages[-1]["payload"]
    assert payload["id"] == "row-id"
    assert payload["tenant_id"] == "tenant-id"
    assert payload["app_id"] == "app-id"
    assert payload["workflow_run_id"] == "run-id"
    assert payload["triggered_from"] == WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN.value
    assert payload["created_by_role"] == CreatorUserRole.ACCOUNT.value
    assert payload["created_by"] == "user-id"
    assert payload["outputs"] == {"answer": "world"}


def test_repository_save_execution_data_is_noop() -> None:
    publisher = FakePublisher()
    repo = _repo(publisher.publish)

    repo.save_execution_data(_execution())

    assert publisher.messages == []


def test_repository_fail_open_keeps_execution_cached_and_does_not_roll_back_state_version() -> None:
    calls = 0

    def publish(event: dict[str, Any]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("broker down")

    repo = _repo(publish)
    execution = _execution()

    repo.save(execution)
    repo.save(execution)

    assert repo.get_by_workflow_run("run-id") == [execution]
    assert repo._state_versions["row-id"] == 2


def test_repository_publishes_outside_lock() -> None:
    class RecordingLock:
        def __init__(self) -> None:
            self.locked = False

        def __enter__(self) -> "RecordingLock":
            self.locked = True
            return self

        def __exit__(self, *args: object) -> None:
            self.locked = False

    recording_lock = RecordingLock()
    publish_lock_states: list[bool] = []

    def publish(event: dict[str, Any]) -> None:
        publish_lock_states.append(recording_lock.locked)

    repo = _repo(publish)
    repo._lock = recording_lock

    repo.save(_execution())

    assert publish_lock_states == [False]


def test_publisher_reuses_connection_for_successive_publishes(monkeypatch) -> None:
    connection = FakeStompConnection()
    _install_fake_stomp(monkeypatch, [connection])
    publisher = WorkflowNodeExecutionActiveMQPublisher(pool_size=1, max_retries=0)

    publisher.publish(_event())
    publisher.publish(_event())

    assert len(connection.sent) == 2
    assert connection.disconnect_calls == 0


def test_publisher_resets_connection_and_retries_on_send_failure(monkeypatch) -> None:
    failed_connection = FakeStompConnection(fail_send_once=True)
    retry_connection = FakeStompConnection()
    _install_fake_stomp(monkeypatch, [failed_connection, retry_connection])
    publisher = WorkflowNodeExecutionActiveMQPublisher(pool_size=1, max_retries=1)

    publisher.publish(_event())

    assert failed_connection.disconnect_calls == 1
    assert len(retry_connection.sent) == 1


def test_publisher_round_robins_pool_slots(monkeypatch) -> None:
    first_connection = FakeStompConnection()
    second_connection = FakeStompConnection()
    _install_fake_stomp(monkeypatch, [first_connection, second_connection])
    publisher = WorkflowNodeExecutionActiveMQPublisher(pool_size=2, max_retries=0)

    publisher.publish(_event())
    publisher.publish(_event())
    publisher.publish(_event())

    assert len(first_connection.sent) == 2
    assert len(second_connection.sent) == 1


def test_publisher_logs_slow_publish_timing(monkeypatch, caplog) -> None:
    connection = FakeStompConnection()
    _install_fake_stomp(monkeypatch, [connection])
    timestamps = iter([0.0, 0.1, 0.7])
    monkeypatch.setattr(
        "core.repositories.workflow_node_execution_activemq_repository.time.perf_counter",
        lambda: next(timestamps),
    )
    publisher = WorkflowNodeExecutionActiveMQPublisher(
        pool_size=1,
        max_retries=0,
        slow_log_threshold=0.5,
    )

    with caplog.at_level(logging.WARNING):
        publisher.publish(_event())

    assert "Slow ActiveMQ workflow log publish" in caplog.text
    assert "pool_slot=0" in caplog.text
