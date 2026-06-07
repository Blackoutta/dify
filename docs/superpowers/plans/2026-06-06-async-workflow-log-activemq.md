# Async Workflow Node Log ActiveMQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish non-debugging workflow node execution logs to ActiveMQ asynchronously while keeping `workflow_runs` synchronous, preserving debugging sync writes, runtime repository semantics, and complete tracing.

**Architecture:** Keep `SQLAlchemyWorkflowExecutionRepository` synchronous for all invocation types. Add a workflow-node-log publisher abstraction with an ActiveMQ STOMP implementation and explicit node `WorkflowLogWriteMode`; only `SQLAlchemyWorkflowNodeExecutionRepository` switches between sync DB persistence and async node-event publication. The ActiveMQ publisher is a process-level singleton with a small configurable STOMP connection pool per API worker, bounded reconnect retry, startup warm-up, and shutdown cleanup. Tracing receives JSON-safe node execution runtime snapshots so all providers, including Arize/Phoenix, do not depend on async node DB persistence.

**Tech Stack:** Python, SQLAlchemy, Pydantic, Flask/Celery, ActiveMQ STOMP via optional `stomp.py`, pytest, unittest.mock.

---

## Important Scope Correction

`workflow_runs` are **not** async in this plan. They remain synchronously written to DB because existing response, app-log, and workflow-level tracing paths rely on them as the canonical run anchor. Only `workflow_node_executions` are published asynchronously for non-debugging production invocations.

## File Structure

Create:
- `api/core/workflow/log_publisher/__init__.py` — exports node publisher types and factory.
- `api/core/workflow/log_publisher/entities.py` — JSON-safe node event envelope, node write mode enum, payload helpers, trace snapshot DTOs.
- `api/core/workflow/log_publisher/publisher.py` — `WorkflowLogPublisher` protocol and no-op implementation.
- `api/core/workflow/log_publisher/activemq_publisher.py` — thread-safe STOMP publisher with startup warm-up, configurable per-worker connection pool, bounded reconnect retry, and close support.
- `api/core/workflow/log_publisher/factory.py` — config-driven process-level singleton publisher creation keyed by ActiveMQ configuration including pool size.
- `api/core/ops/workflow_trace_snapshots.py` — provider-neutral adapters from JSON node snapshots to provider-friendly objects.
- `api/tests/unit_tests/core/workflow/log_publisher/test_entities.py`
- `api/tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py`
- `api/tests/unit_tests/core/workflow/log_publisher/test_factory.py`
- `api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py`

Modify:
- `api/configs/feature/__init__.py` — add workflow node log async/ActiveMQ config.
- `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py` — add node write mode/publisher, async save path, cached running lookup, and node trace snapshot accessor.
- `api/core/workflow/repositories/workflow_node_execution_repository.py` — add optional cached/snapshot method declarations if needed by type checking.
- `api/core/workflow/workflow_cycle_manager.py` — pass node execution snapshots to trace tasks after terminal node updates.
- `api/core/ops/entities/trace_entity.py` — add JSON-safe node trace snapshot field to `WorkflowTraceInfo`.
- `api/core/ops/ops_trace_manager.py` — preserve sync `WorkflowRun` lookup but include node snapshots in `WorkflowTraceInfo` when present.
- Trace providers:
  - `api/core/ops/langfuse_trace/langfuse_trace.py`
  - `api/core/ops/langsmith_trace/langsmith_trace.py`
  - `api/core/ops/weave_trace/weave_trace.py`
  - `api/core/ops/opik_trace/opik_trace.py`
  - `api/core/ops/aliyun_trace/aliyun_trace.py`
  - `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
- App generator repository construction sites:
  - `api/core/app/apps/workflow/app_generator.py`
  - `api/core/app/apps/advanced_chat/app_generator.py`
- Env/dependency files if dependency policy requires:
  - `api/pyproject.toml`
  - `api/uv.lock`
  - `.env.example` or deployment env examples if present.

Do not modify `SQLAlchemyWorkflowExecutionRepository` for async publishing. Existing workflow run tests should continue to pass without async-specific behavior.

## Task 1: Config and Core Node Publisher Entities

**Files:**
- Modify: `api/configs/feature/__init__.py`
- Create: `api/core/workflow/log_publisher/__init__.py`
- Create: `api/core/workflow/log_publisher/entities.py`
- Test: `api/tests/unit_tests/core/workflow/log_publisher/test_entities.py`

- [ ] **Step 1: Write failing serialization/config tests**

Create `api/tests/unit_tests/core/workflow/log_publisher/test_entities.py`:

```python
from datetime import datetime
from decimal import Decimal

from core.workflow.log_publisher.entities import (
    NodeExecutionTraceSnapshot,
    WorkflowLogEvent,
    WorkflowLogEventType,
    WorkflowLogWriteMode,
    dump_json_safe,
)


def test_dump_json_safe_serializes_datetime_enum_and_decimal():
    payload = dump_json_safe(
        {
            "created_at": datetime(2026, 6, 6, 1, 2, 3),
            "mode": WorkflowLogWriteMode.ASYNC,
            "price": Decimal("1.25"),
        }
    )

    assert payload == {
        "created_at": "2026-06-06T01:02:03Z",
        "mode": "async",
        "price": 1.25,
    }


def test_node_execution_event_is_json_serializable():
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
        payload={"workflow_run_id": "run-1", "created_at": datetime(2026, 6, 6, 1, 2, 3)},
    )

    dumped = event.model_dump(mode="json")

    assert dumped["event_type"] == "workflow_node_execution.upsert"
    assert dumped["schema_version"] == 1
    assert dumped["payload"] == {"workflow_run_id": "run-1", "created_at": "2026-06-06T01:02:03Z"}
    assert dumped["event_id"]
    assert dumped["created_at"].endswith("Z")


def test_node_trace_snapshot_is_json_safe():
    snapshot = NodeExecutionTraceSnapshot(
        id="record-1",
        workflow_run_id="run-1",
        node_execution_id="node-exec-1",
        node_id="llm",
        node_type="llm",
        title="LLM",
        inputs={"query": "hello"},
        process_data={"prompts": []},
        outputs={"text": "world"},
        status="succeeded",
        error=None,
        elapsed_time=1.0,
        metadata={"total_tokens": 10},
        created_at=datetime(2026, 6, 6, 1, 2, 3),
        finished_at=datetime(2026, 6, 6, 1, 2, 4),
    )

    dumped = snapshot.model_dump(mode="json")

    assert dumped["created_at"] == "2026-06-06T01:02:03Z"
    assert dumped["finished_at"] == "2026-06-06T01:02:04Z"
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/log_publisher/test_entities.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'core.workflow.log_publisher'`.

- [ ] **Step 3: Add config fields**

In `api/configs/feature/__init__.py`, add fields to `WorkflowConfig` after `MAX_VARIABLE_SIZE`:

```python
    WORKFLOW_LOG_ASYNC_ENABLED: bool = Field(
        description="Whether to publish non-debugging workflow node execution logs asynchronously through a message queue.",
        default=False,
    )

    WORKFLOW_LOG_QUEUE_PROVIDER: str = Field(
        description="Queue provider for async workflow node execution logs. This version supports 'activemq'.",
        default="activemq",
    )

    WORKFLOW_LOG_ACTIVEMQ_HOST: str = Field(
        description="ActiveMQ STOMP host for async workflow node execution log publishing.",
        default="localhost",
    )

    WORKFLOW_LOG_ACTIVEMQ_PORT: PositiveInt = Field(
        description="ActiveMQ STOMP port for async workflow node execution log publishing.",
        default=61613,
    )

    WORKFLOW_LOG_ACTIVEMQ_USERNAME: str | None = Field(
        description="ActiveMQ username for async workflow node execution log publishing.",
        default=None,
    )

    WORKFLOW_LOG_ACTIVEMQ_PASSWORD: str | None = Field(
        description="ActiveMQ password for async workflow node execution log publishing.",
        default=None,
    )

    WORKFLOW_LOG_ACTIVEMQ_DESTINATION: str = Field(
        description="ActiveMQ queue destination for workflow node execution events.",
        default="/queue/dify.workflow.logs",
    )

    WORKFLOW_LOG_PUBLISH_TIMEOUT: PositiveFloat = Field(
        description="Maximum seconds allowed for one workflow node execution log publish attempt.",
        default=0.2,
    )

    WORKFLOW_LOG_PUBLISH_MAX_RETRIES: NonNegativeInt = Field(
        description="Number of retries after the initial ActiveMQ workflow node execution log publish attempt fails.",
        default=1,
    )
```

Ensure `PositiveFloat` and `NonNegativeInt` are imported from `pydantic` if they are not already imported.

- [ ] **Step 4: Implement entities**

Create `api/core/workflow/log_publisher/entities.py`:

```python
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_serializer


class WorkflowLogWriteMode(StrEnum):
    SYNC = "sync"
    ASYNC = "async"


class WorkflowLogEventType(StrEnum):
    WORKFLOW_NODE_EXECUTION_UPSERT = "workflow_node_execution.upsert"


def _serialize_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        return value.isoformat() + "Z"
    return value.astimezone(UTC).replace(tzinfo=None).isoformat() + "Z"


def dump_json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return _serialize_datetime(value)
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(dump_json_safe(k)): dump_json_safe(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [dump_json_safe(item) for item in value]
    return value


class WorkflowLogEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: WorkflowLogEventType
    schema_version: int = 1
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC).replace(tzinfo=None))
    payload: dict[str, Any]

    model_config = ConfigDict(use_enum_values=True)

    @field_serializer("created_at")
    def serialize_created_at(self, value: datetime) -> str:
        return _serialize_datetime(value)

    @classmethod
    def create(cls, *, event_type: WorkflowLogEventType, payload: dict[str, Any]) -> "WorkflowLogEvent":
        return cls(event_type=event_type, payload=dump_json_safe(payload))


class NodeExecutionTraceSnapshot(BaseModel):
    id: str
    workflow_run_id: str
    node_execution_id: str | None = None
    node_id: str
    node_type: str
    title: str
    inputs: dict[str, Any] | None = None
    process_data: dict[str, Any] | None = None
    outputs: dict[str, Any] | None = None
    status: str
    error: str | None = None
    elapsed_time: int | float | None = None
    metadata: dict[str, Any] | None = None
    created_at: datetime
    finished_at: datetime | None = None

    @field_serializer("created_at", "finished_at")
    def serialize_datetime_fields(self, value: datetime | None) -> str | None:
        return _serialize_datetime(value) if value else None
```

Create `api/core/workflow/log_publisher/__init__.py`:

```python
from core.workflow.log_publisher.entities import (
    NodeExecutionTraceSnapshot,
    WorkflowLogEvent,
    WorkflowLogEventType,
    WorkflowLogWriteMode,
)

__all__ = [
    "NodeExecutionTraceSnapshot",
    "WorkflowLogEvent",
    "WorkflowLogEventType",
    "WorkflowLogWriteMode",
]
```

- [ ] **Step 5: Run tests**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/log_publisher/test_entities.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/configs/feature/__init__.py api/core/workflow/log_publisher api/tests/unit_tests/core/workflow/log_publisher/test_entities.py
git commit -m "feat: add workflow node log publisher entities"
```

## Task 2: Publisher Protocol, Factory, and ActiveMQ STOMP Publisher

**Files:**
- Create: `api/core/workflow/log_publisher/publisher.py`
- Create: `api/core/workflow/log_publisher/activemq_publisher.py`
- Create: `api/core/workflow/log_publisher/factory.py`
- Modify: `api/core/workflow/log_publisher/__init__.py`
- Test: `api/tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py`
- Test: `api/tests/unit_tests/core/workflow/log_publisher/test_factory.py`

- [ ] **Step 1: Write failing publisher tests**

Create `api/tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py`:

```python
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
```

Create `api/tests/unit_tests/core/workflow/log_publisher/test_factory.py`:

```python
from unittest.mock import Mock

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.factory import create_workflow_log_publisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher


def test_factory_returns_noop_when_async_disabled():
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=False)

    publisher = create_workflow_log_publisher(config)

    assert isinstance(publisher, NoopWorkflowLogPublisher)


def test_factory_returns_activemq_when_enabled():
    config = Mock(
        WORKFLOW_LOG_ASYNC_ENABLED=True,
        WORKFLOW_LOG_QUEUE_PROVIDER="activemq",
        WORKFLOW_LOG_ACTIVEMQ_HOST="mq.local",
        WORKFLOW_LOG_ACTIVEMQ_PORT=61613,
        WORKFLOW_LOG_ACTIVEMQ_USERNAME="user",
        WORKFLOW_LOG_ACTIVEMQ_PASSWORD="pass",
        WORKFLOW_LOG_ACTIVEMQ_DESTINATION="/queue/dify.workflow.logs",
        WORKFLOW_LOG_PUBLISH_TIMEOUT=0.2,
    )

    publisher = create_workflow_log_publisher(config)

    assert isinstance(publisher, ActiveMQWorkflowLogPublisher)


def test_factory_rejects_unsupported_provider():
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=True, WORKFLOW_LOG_QUEUE_PROVIDER="kafka")

    try:
        create_workflow_log_publisher(config)
    except ValueError as exc:
        assert "Unsupported workflow log queue provider" in str(exc)
    else:
        raise AssertionError("expected ValueError")
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py tests/unit_tests/core/workflow/log_publisher/test_factory.py -v
```

Expected: FAIL because modules are missing.

- [ ] **Step 3: Implement protocol, ActiveMQ publisher, and factory**

Create `api/core/workflow/log_publisher/publisher.py`:

```python
from __future__ import annotations

from typing import Protocol

from core.workflow.log_publisher.entities import WorkflowLogEvent


class WorkflowLogPublisher(Protocol):
    def publish(self, event: WorkflowLogEvent) -> None:
        raise NotImplementedError


class NoopWorkflowLogPublisher:
    def publish(self, event: WorkflowLogEvent) -> None:
        return None
```

Create `api/core/workflow/log_publisher/activemq_publisher.py`:

```python
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
            group_id = event.payload.get("workflow_run_id")
            if group_id:
                headers["JMSXGroupID"] = str(group_id)
            try:
                connection.send(destination=self._destination, body=event.model_dump_json(), headers=headers)
            except Exception:
                self._reset_connection()
                raise

    def _ensure_connection(self):
        if self._connection is not None:
            return self._connection
        try:
            import stomp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("stomp.py is required when async workflow node log publishing is enabled") from exc
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
```

Create `api/core/workflow/log_publisher/factory.py`:

```python
from __future__ import annotations

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher


def create_workflow_log_publisher(config) -> WorkflowLogPublisher:
    if not config.WORKFLOW_LOG_ASYNC_ENABLED:
        return NoopWorkflowLogPublisher()
    provider = str(config.WORKFLOW_LOG_QUEUE_PROVIDER).lower()
    if provider != "activemq":
        raise ValueError(f"Unsupported workflow log queue provider: {config.WORKFLOW_LOG_QUEUE_PROVIDER}")
    return ActiveMQWorkflowLogPublisher(
        host=config.WORKFLOW_LOG_ACTIVEMQ_HOST,
        port=config.WORKFLOW_LOG_ACTIVEMQ_PORT,
        username=config.WORKFLOW_LOG_ACTIVEMQ_USERNAME,
        password=config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD,
        destination=config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
        timeout=config.WORKFLOW_LOG_PUBLISH_TIMEOUT,
    )
```

Update `api/core/workflow/log_publisher/__init__.py` to export factory and publisher classes.

- [ ] **Step 4: Run tests and commit**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/log_publisher -v
git add api/core/workflow/log_publisher api/tests/unit_tests/core/workflow/log_publisher
git commit -m "feat: add ActiveMQ workflow node log publisher"
```

Expected: tests PASS, commit succeeds.

## Task 3: Node Execution Repository Async Save and Cache Semantics

**Files:**
- Modify: `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`
- Test: `api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py`

- [ ] **Step 1: Add failing tests**

Append tests covering:

```python
from core.workflow.log_publisher.entities import WorkflowLogEventType, WorkflowLogWriteMode


def test_async_save_publishes_node_execution_and_updates_cache(repository, mocker):
    publisher = mocker.Mock()
    repository._write_mode = WorkflowLogWriteMode.ASYNC
    repository._workflow_log_publisher = publisher
    execution = _create_domain_node_execution(status=WorkflowNodeExecutionStatus.RUNNING)

    repository.save(execution)

    publisher.publish.assert_called_once()
    event = publisher.publish.call_args.args[0]
    assert event.event_type == WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT
    assert event.payload["workflow_run_id"] == execution.workflow_execution_id
    assert event.payload["triggered_from"] == WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN
    assert repository.get_by_node_execution_id(execution.node_execution_id).node_execution_id == execution.node_execution_id


def test_async_save_fail_open_still_updates_cache(repository, mocker):
    publisher = mocker.Mock()
    publisher.publish.side_effect = RuntimeError("broker down")
    repository._write_mode = WorkflowLogWriteMode.ASYNC
    repository._workflow_log_publisher = publisher
    execution = _create_domain_node_execution(status=WorkflowNodeExecutionStatus.RUNNING)

    repository.save(execution)

    assert repository.get_by_node_execution_id(execution.node_execution_id).node_execution_id == execution.node_execution_id


def test_async_get_running_executions_returns_cached_running_nodes(repository, mocker):
    repository._write_mode = WorkflowLogWriteMode.ASYNC
    repository._workflow_log_publisher = mocker.Mock()
    running = _create_domain_node_execution(status=WorkflowNodeExecutionStatus.RUNNING)
    succeeded = _create_domain_node_execution(id="record-2", node_execution_id="node-exec-2", status=WorkflowNodeExecutionStatus.SUCCEEDED)

    repository.save(running)
    repository.save(succeeded)

    results = repository.get_running_executions(running.workflow_execution_id)

    assert [item.node_execution_id for item in results] == [running.node_execution_id]
```

If needed, add `_create_domain_node_execution()` using existing test patterns in this file.

- [ ] **Step 2: Run tests to verify failure**

```bash
cd api && uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -v
```

Expected: FAIL because async path and cached running lookup do not exist.

- [ ] **Step 3: Implement node async save**

In `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`:

- Add constructor parameters with backward-compatible defaults:

```python
        write_mode: WorkflowLogWriteMode = WorkflowLogWriteMode.SYNC,
        workflow_log_publisher: WorkflowLogPublisher | None = None,
```

- Set:

```python
        self._write_mode = write_mode
        self._workflow_log_publisher = workflow_log_publisher or NoopWorkflowLogPublisher()
```

- Add `_node_execution_payload(db_model)`:

```python
    def _node_execution_payload(self, db_model: WorkflowNodeExecutionModel) -> dict:
        return {
            "id": db_model.id,
            "tenant_id": db_model.tenant_id,
            "app_id": db_model.app_id,
            "workflow_id": db_model.workflow_id,
            "workflow_run_id": db_model.workflow_run_id,
            "triggered_from": db_model.triggered_from,
            "node_execution_id": db_model.node_execution_id,
            "node_id": db_model.node_id,
            "node_type": db_model.node_type,
            "title": db_model.title,
            "index": db_model.index,
            "predecessor_node_id": db_model.predecessor_node_id,
            "inputs": db_model.inputs_dict,
            "process_data": db_model.process_data_dict,
            "outputs": db_model.outputs_dict,
            "status": db_model.status,
            "error": db_model.error,
            "elapsed_time": db_model.elapsed_time,
            "execution_metadata": db_model.execution_metadata_dict,
            "created_by_role": db_model.created_by_role,
            "created_by": db_model.created_by,
            "created_at": db_model.created_at,
            "finished_at": db_model.finished_at,
        }
```

- In `save()`, after `db_model = self.to_db_model(execution)`, add async branch:

```python
        if self._write_mode == WorkflowLogWriteMode.ASYNC:
            try:
                self._workflow_log_publisher.publish(
                    WorkflowLogEvent.create(
                        event_type=WorkflowLogEventType.WORKFLOW_NODE_EXECUTION_UPSERT,
                        payload=self._node_execution_payload(db_model),
                    )
                )
            except Exception:
                logger.warning(
                    "Failed to publish workflow node execution log event",
                    exc_info=True,
                    extra={"workflow_run_id": db_model.workflow_run_id, "node_execution_id": db_model.node_execution_id},
                )
            if db_model.node_execution_id:
                self._node_execution_cache[db_model.node_execution_id] = db_model
            return
```

Keep the existing synchronous DB path unchanged.

- [ ] **Step 4: Implement cached running lookup and snapshot accessors**

Update `get_running_executions()` so async mode returns matching cached running nodes instead of requiring DB rows:

```python
        cached_models = [
            model
            for model in self._node_execution_cache.values()
            if model.workflow_run_id == workflow_run_id
            and model.tenant_id == self._tenant_id
            and model.status == WorkflowNodeExecutionStatus.RUNNING
        ]
        if self._app_id:
            cached_models = [model for model in cached_models if model.app_id == self._app_id]

        if self._write_mode == WorkflowLogWriteMode.ASYNC:
            return [self._to_domain_model(model) for model in cached_models]
```

For synchronous mode, keep the existing DB query and merge cached/DB rows by `node_execution_id or id` before converting to domain models.

Add snapshot methods:

```python
    def get_cached_executions_by_workflow_run(self, workflow_run_id: str) -> Sequence[WorkflowNodeExecution]:
        models = [
            model
            for model in self._node_execution_cache.values()
            if model.workflow_run_id == workflow_run_id and model.tenant_id == self._tenant_id
        ]
        if self._app_id:
            models = [model for model in models if model.app_id == self._app_id]
        models.sort(key=lambda model: (model.index or 0, model.created_at or datetime.datetime.min, model.id))
        return [self._to_domain_model(model) for model in models]

    def to_trace_snapshot(self, execution: WorkflowNodeExecution) -> dict:
        db_model = self.to_db_model(execution)
        return NodeExecutionTraceSnapshot(
            id=db_model.id,
            workflow_run_id=db_model.workflow_run_id,
            node_execution_id=db_model.node_execution_id,
            node_id=db_model.node_id,
            node_type=str(db_model.node_type),
            title=db_model.title,
            inputs=db_model.inputs_dict,
            process_data=db_model.process_data_dict,
            outputs=db_model.outputs_dict,
            status=str(db_model.status),
            error=db_model.error,
            elapsed_time=db_model.elapsed_time,
            metadata=db_model.execution_metadata_dict,
            created_at=db_model.created_at,
            finished_at=db_model.finished_at,
        ).model_dump(mode="json")
```

- [ ] **Step 5: Run tests and commit**

```bash
cd api && uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -v
git add api/core/repositories/sqlalchemy_workflow_node_execution_repository.py api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py
git commit -m "feat: publish workflow node logs asynchronously"
```

Expected: tests PASS, commit succeeds.

## Task 4: Wire Node Async Mode in App Generators

**Files:**
- Modify: `api/core/app/apps/workflow/app_generator.py`
- Modify: `api/core/app/apps/advanced_chat/app_generator.py`

- [ ] **Step 1: Add node write-mode helper**

In both files, import:

```python
from configs import dify_config
from core.workflow.log_publisher import WorkflowLogWriteMode, create_workflow_log_publisher
```

Add helper:

```python
def _workflow_node_log_write_mode_for_invoke(invoke_from: InvokeFrom) -> WorkflowLogWriteMode:
    if invoke_from == InvokeFrom.DEBUGGER:
        return WorkflowLogWriteMode.SYNC
    if not dify_config.WORKFLOW_LOG_ASYNC_ENABLED:
        return WorkflowLogWriteMode.SYNC
    return WorkflowLogWriteMode.ASYNC
```

- [ ] **Step 2: Pass mode only to node repository**

For full workflow runs, keep `SQLAlchemyWorkflowExecutionRepository(...)` unchanged except existing `triggered_from` logic.

Before constructing `SQLAlchemyWorkflowNodeExecutionRepository`, add:

```python
        workflow_node_log_write_mode = _workflow_node_log_write_mode_for_invoke(invoke_from)
        workflow_log_publisher = create_workflow_log_publisher(dify_config)
```

Pass only to node repository:

```python
            write_mode=workflow_node_log_write_mode,
            workflow_log_publisher=workflow_log_publisher,
```

Single-step debugging paths must omit these parameters or pass `write_mode=WorkflowLogWriteMode.SYNC`.

- [ ] **Step 3: Run focused tests and commit**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/test_workflow_cycle_manager.py tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -v
git add api/core/app/apps/workflow/app_generator.py api/core/app/apps/advanced_chat/app_generator.py
git commit -m "feat: enable async workflow node log routing"
```

Expected: tests PASS, commit succeeds.

## Task 5: JSON-Safe Node Trace Snapshot Plumbing

**Files:**
- Modify: `api/core/workflow/workflow_cycle_manager.py`
- Modify: `api/core/ops/entities/trace_entity.py`
- Modify: `api/core/ops/ops_trace_manager.py`
- Test: `api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py`
- Test: `api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py`

- [ ] **Step 1: Write failing snapshot tests**

Create `api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py`:

```python
from core.ops.entities.trace_entity import WorkflowTraceInfo


def test_workflow_trace_info_keeps_node_snapshots_json_safe():
    trace_info = WorkflowTraceInfo(
        workflow_data={},
        conversation_id=None,
        workflow_id="workflow-1",
        tenant_id="tenant-1",
        workflow_run_id="run-1",
        workflow_run_elapsed_time=1.2,
        workflow_run_status="succeeded",
        workflow_run_inputs={"query": "hello"},
        workflow_run_outputs={"answer": "world"},
        workflow_run_version="1",
        error=None,
        total_tokens=10,
        file_list=[],
        query="hello",
        metadata={"app_id": "app-1"},
        node_execution_snapshots=[
            {
                "id": "record-1",
                "workflow_run_id": "run-1",
                "node_execution_id": "node-exec-1",
                "node_id": "llm",
                "node_type": "llm",
                "title": "LLM",
                "inputs": {"query": "hello"},
                "process_data": {"prompts": []},
                "outputs": {"text": "world"},
                "status": "succeeded",
                "error": None,
                "elapsed_time": 1.0,
                "metadata": {"total_tokens": 10},
                "created_at": "2026-06-06T01:02:03Z",
                "finished_at": "2026-06-06T01:02:04Z",
            }
        ],
    )

    restored = WorkflowTraceInfo.model_validate_json(trace_info.model_dump_json())

    assert restored.node_execution_snapshots[0]["node_execution_id"] == "node-exec-1"
```

Add a `WorkflowCycleManager` test asserting `TraceTask.node_execution_snapshots` is populated after node terminal updates.

- [ ] **Step 2: Run tests to verify failure**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_workflow_trace_snapshots.py tests/unit_tests/core/workflow/test_workflow_cycle_manager.py -v
```

Expected: FAIL because snapshot fields/plumbing do not exist.

- [ ] **Step 3: Add node snapshot field to WorkflowTraceInfo and TraceTask**

In `api/core/ops/entities/trace_entity.py`, add to `WorkflowTraceInfo`:

```python
    node_execution_snapshots: list[dict[str, Any]] = []
```

In `api/core/ops/ops_trace_manager.py`, extend `TraceTask.__init__`:

```python
        node_execution_snapshots: list[dict[str, Any]] | None = None,
```

Set:

```python
        self.node_execution_snapshots = node_execution_snapshots or []
```

When constructing `WorkflowTraceInfo` from the existing synchronous `WorkflowRun` DB row, pass:

```python
                node_execution_snapshots=self.node_execution_snapshots,
```

Do not remove the existing synchronous `WorkflowRun` DB query; workflow runs remain sync.

- [ ] **Step 4: Pass node snapshots from WorkflowCycleManager**

In `WorkflowCycleManager`, add helper:

```python
    def _node_trace_snapshots(self, workflow_execution_id: str) -> list[dict]:
        if not hasattr(self._workflow_node_execution_repository, "get_cached_executions_by_workflow_run"):
            return []
        if not hasattr(self._workflow_node_execution_repository, "to_trace_snapshot"):
            return []
        node_executions = self._workflow_node_execution_repository.get_cached_executions_by_workflow_run(
            workflow_execution_id
        )
        return [self._workflow_node_execution_repository.to_trace_snapshot(node) for node in node_executions]
```

Before every workflow `TraceTask(...)` creation in success, partial success, and failed handlers, compute after terminal node updates:

```python
            node_execution_snapshots = self._node_trace_snapshots(workflow_execution.id_)
```

Pass:

```python
                    node_execution_snapshots=node_execution_snapshots,
```

- [ ] **Step 5: Run tests and commit**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_workflow_trace_snapshots.py tests/unit_tests/core/workflow/test_workflow_cycle_manager.py -v
git add api/core/workflow/workflow_cycle_manager.py api/core/ops/entities/trace_entity.py api/core/ops/ops_trace_manager.py api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py
git commit -m "feat: pass workflow node trace snapshots"
```

Expected: tests PASS, commit succeeds.

## Task 6: Update Trace Providers to Prefer Node Snapshots

**Files:**
- Create: `api/core/ops/workflow_trace_snapshots.py`
- Modify trace providers listed in File Structure.
- Test: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`

- [ ] **Step 1: Write failing Arize/Phoenix snapshot test**

In `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`, add a test that builds `WorkflowTraceInfo` with `node_execution_snapshots`, makes `_get_workflow_nodes()` return `[]`, and asserts Arize/Phoenix still emits at least one node span. Follow existing constructor/mocking patterns in that file.

Minimum snapshot fixture:

```python
workflow_trace_info.node_execution_snapshots = [
    {
        "id": "record-1",
        "workflow_run_id": workflow_trace_info.workflow_run_id,
        "node_execution_id": "node-exec-1",
        "node_id": "llm",
        "node_type": "llm",
        "title": "LLM",
        "inputs": {"query": "hello"},
        "process_data": {"prompts": [], "model_mode": "chat", "model_provider": "openai", "model_name": "gpt-4"},
        "outputs": {"text": "world"},
        "status": "succeeded",
        "error": None,
        "elapsed_time": 1.0,
        "metadata": {"total_tokens": 10},
        "created_at": "2026-06-06T01:02:03Z",
        "finished_at": "2026-06-06T01:02:04Z",
    }
]
```

- [ ] **Step 2: Add provider-neutral adapter**

Create `api/core/ops/workflow_trace_snapshots.py`:

```python
from __future__ import annotations

from types import SimpleNamespace
from typing import Any


def workflow_node_snapshot_to_domain_like(snapshot: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        id=snapshot.get("id"),
        workflow_execution_id=snapshot.get("workflow_run_id"),
        workflow_run_id=snapshot.get("workflow_run_id"),
        node_execution_id=snapshot.get("node_execution_id"),
        node_id=snapshot.get("node_id"),
        node_type=snapshot.get("node_type"),
        title=snapshot.get("title"),
        inputs=snapshot.get("inputs") or {},
        process_data=snapshot.get("process_data") or {},
        outputs=snapshot.get("outputs") or {},
        status=snapshot.get("status"),
        error=snapshot.get("error"),
        elapsed_time=snapshot.get("elapsed_time") or 0,
        metadata=snapshot.get("metadata") or {},
        execution_metadata=snapshot.get("metadata") or {},
        created_at=snapshot.get("created_at"),
        finished_at=snapshot.get("finished_at"),
    )
```

- [ ] **Step 3: Update providers**

For Langfuse/Langsmith/Weave/Opik/Aliyun, before creating a fresh node repository/querying DB:

```python
from core.ops.workflow_trace_snapshots import workflow_node_snapshot_to_domain_like

workflow_node_executions = None
if getattr(trace_info, "node_execution_snapshots", None):
    workflow_node_executions = [
        workflow_node_snapshot_to_domain_like(snapshot)
        for snapshot in trace_info.node_execution_snapshots
    ]

if workflow_node_executions is None:
    workflow_node_executions = workflow_node_execution_repository.get_by_workflow_run(
        workflow_run_id=trace_info.workflow_run_id
    )
```

For Arize/Phoenix, replace `_get_workflow_nodes()` usage with snapshots when present:

```python
if trace_info.node_execution_snapshots:
    workflow_nodes = [workflow_node_snapshot_to_domain_like(snapshot) for snapshot in trace_info.node_execution_snapshots]
else:
    workflow_nodes = list(self._get_workflow_nodes(trace_info.workflow_run_id))
```

Make helper functions tolerate dict-valued `inputs`, `outputs`, `process_data`, and `execution_metadata` in addition to DB JSON strings.

- [ ] **Step 4: Run tests and commit**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_arize_phoenix_trace.py tests/unit_tests/core/ops/test_workflow_trace_snapshots.py -v
git add api/core/ops api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py
git commit -m "feat: use node snapshots for workflow tracing"
```

Expected: tests PASS, commit succeeds.

## Task 7: Dependency Declaration and Environment Notes

**Files:**
- Modify: `api/pyproject.toml` if dependency policy requires.
- Modify: `api/uv.lock` if dependency policy requires lock updates.
- Modify: `.env.example` or deployment env example files if present.

- [ ] **Step 1: Check dependency policy**

```bash
cd api && rg -n "\[project.optional-dependencies\]|stomp|celery" pyproject.toml
```

Expected: See existing dependency layout.

- [ ] **Step 2: Add `stomp.py` dependency**

If the project uses mandatory dependencies only, add to `api/pyproject.toml` dependencies:

```toml
"stomp.py>=8.1.0",
```

If optional dependencies are preferred, add:

```toml
[project.optional-dependencies]
workflow-log-activemq = ["stomp.py>=8.1.0"]
```

Do not import `stomp` outside `ActiveMQWorkflowLogPublisher._ensure_connection()`.

- [ ] **Step 3: Add env examples**

Find env example file:

```bash
find . -maxdepth 3 -name '*env*example*' -o -name '.env.example'
```

Add:

```env
# Async workflow node execution log publishing. Workflow runs remain synchronous.
WORKFLOW_LOG_ASYNC_ENABLED=false
WORKFLOW_LOG_QUEUE_PROVIDER=activemq
WORKFLOW_LOG_ACTIVEMQ_HOST=localhost
WORKFLOW_LOG_ACTIVEMQ_PORT=61613
WORKFLOW_LOG_ACTIVEMQ_USERNAME=
WORKFLOW_LOG_ACTIVEMQ_PASSWORD=
WORKFLOW_LOG_ACTIVEMQ_DESTINATION=/queue/dify.workflow.logs
WORKFLOW_LOG_PUBLISH_TIMEOUT=0.2
WORKFLOW_LOG_PUBLISH_MAX_RETRIES=1
```

- [ ] **Step 4: Update lock if required and commit**

```bash
cd api && uv lock
git add api/pyproject.toml api/uv.lock .
git commit -m "chore: document async workflow node log configuration"
```

Expected: lock updates only if dependency changed.

## Task 8: ActiveMQ Publisher Reliability Hardening

**Files:**
- Modify: `api/configs/feature/__init__.py`
- Modify: `api/core/workflow/log_publisher/activemq_publisher.py`
- Modify: `api/core/workflow/log_publisher/factory.py`
- Modify: `api/core/workflow/log_publisher/publisher.py`
- Modify: `api/tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py`
- Modify: `api/tests/unit_tests/core/workflow/log_publisher/test_factory.py`

**Purpose:** Fix the observed second-pressure-round message loss caused by per-run publisher allocation and weak stale connection recovery. Start with one process-level publisher per API worker, bounded retry, startup warm-up, and explicit close; later high-concurrency testing extended this publisher with a configurable per-worker STOMP connection pool.

- [ ] **Step 1: Write failing retry and close tests**

Append these tests to `api/tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py`:

```python
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

    try:
        publisher.publish(event)
    except RuntimeError as exc:
        assert "broker down" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")

    assert publisher._connection is None


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
```

- [ ] **Step 2: Write failing singleton factory tests**

Append these tests to `api/tests/unit_tests/core/workflow/log_publisher/test_factory.py`:

```python
def _activemq_config(**overrides):
    values = {
        "WORKFLOW_LOG_ASYNC_ENABLED": True,
        "WORKFLOW_LOG_QUEUE_PROVIDER": "activemq",
        "WORKFLOW_LOG_ACTIVEMQ_HOST": "mq.local",
        "WORKFLOW_LOG_ACTIVEMQ_PORT": 61613,
        "WORKFLOW_LOG_ACTIVEMQ_USERNAME": "user",
        "WORKFLOW_LOG_ACTIVEMQ_PASSWORD": "pass",
        "WORKFLOW_LOG_ACTIVEMQ_DESTINATION": "/queue/dify.workflow.logs",
        "WORKFLOW_LOG_PUBLISH_TIMEOUT": 0.2,
        "WORKFLOW_LOG_PUBLISH_MAX_RETRIES": 1,
    }
    values.update(overrides)
    return Mock(**values)


def test_factory_reuses_process_singleton_for_same_activemq_config():
    first = create_workflow_log_publisher(_activemq_config())
    second = create_workflow_log_publisher(_activemq_config())

    assert first is second


def test_factory_creates_new_singleton_when_activemq_config_changes():
    first = create_workflow_log_publisher(_activemq_config(WORKFLOW_LOG_ACTIVEMQ_HOST="mq-a.local"))
    second = create_workflow_log_publisher(_activemq_config(WORKFLOW_LOG_ACTIVEMQ_HOST="mq-b.local"))

    assert first is not second
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py tests/unit_tests/core/workflow/log_publisher/test_factory.py -v
```

Expected: FAIL because `ActiveMQWorkflowLogPublisher.__init__()` does not accept `max_retries`, `close()` is missing, and factory calls return new instances.

- [ ] **Step 4: Add retry configuration**

In `api/configs/feature/__init__.py`, ensure `NonNegativeInt` is imported from `pydantic`, then add this field next to `WORKFLOW_LOG_PUBLISH_TIMEOUT`:

```python
    WORKFLOW_LOG_PUBLISH_MAX_RETRIES: NonNegativeInt = Field(
        description="Number of retries after the initial ActiveMQ workflow node execution log publish attempt fails.",
        default=1,
    )
```

- [ ] **Step 5: Extend publisher protocol with close support**

Update `api/core/workflow/log_publisher/publisher.py`:

```python
from __future__ import annotations

from typing import Protocol

from core.workflow.log_publisher.entities import WorkflowLogEvent


class WorkflowLogPublisher(Protocol):
    def publish(self, event: WorkflowLogEvent) -> None: ...

    def close(self) -> None: ...


class NoopWorkflowLogPublisher:
    def publish(self, event: WorkflowLogEvent) -> None:
        return None

    def close(self) -> None:
        return None
```

- [ ] **Step 6: Implement bounded retry and close in the ActiveMQ publisher**

Update `api/core/workflow/log_publisher/activemq_publisher.py` so the publisher keeps the existing lock but retries the full ensure/send operation:

```python
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
            last_error: Exception | None = None
            for attempt in range(self._max_retries + 1):
                try:
                    connection = self._ensure_connection()
                    connection.send(destination=self._destination, body=body, headers=headers)
                    return
                except Exception as exc:
                    last_error = exc
                    self._reset_connection()
                    if attempt >= self._max_retries:
                        raise
            if last_error is not None:
                raise last_error

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
```

- [ ] **Step 7: Implement process-level singleton factory**

Update `api/core/workflow/log_publisher/factory.py`:

```python
from __future__ import annotations

import atexit
import threading
from dataclasses import dataclass

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher


@dataclass(frozen=True)
class _ActiveMQPublisherConfigKey:
    host: str
    port: int
    username: str | None
    password: str | None
    destination: str
    timeout: float
    max_retries: int


_singleton_lock = threading.RLock()
_singleton_publishers: dict[_ActiveMQPublisherConfigKey, ActiveMQWorkflowLogPublisher] = {}


def create_workflow_log_publisher(config) -> WorkflowLogPublisher:
    if not config.WORKFLOW_LOG_ASYNC_ENABLED:
        return NoopWorkflowLogPublisher()

    provider = str(config.WORKFLOW_LOG_QUEUE_PROVIDER).lower()
    if provider != "activemq":
        raise ValueError(f"Unsupported workflow log queue provider: {config.WORKFLOW_LOG_QUEUE_PROVIDER}")

    key = _ActiveMQPublisherConfigKey(
        host=config.WORKFLOW_LOG_ACTIVEMQ_HOST,
        port=config.WORKFLOW_LOG_ACTIVEMQ_PORT,
        username=config.WORKFLOW_LOG_ACTIVEMQ_USERNAME,
        password=config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD,
        destination=config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
        timeout=config.WORKFLOW_LOG_PUBLISH_TIMEOUT,
        max_retries=config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES,
    )
    with _singleton_lock:
        publisher = _singleton_publishers.get(key)
        if publisher is not None:
            return publisher
        publisher = ActiveMQWorkflowLogPublisher(
            host=key.host,
            port=key.port,
            username=key.username,
            password=key.password,
            destination=key.destination,
            timeout=key.timeout,
            max_retries=key.max_retries,
        )
        _singleton_publishers[key] = publisher
        atexit.register(publisher.close)
        return publisher
```

- [ ] **Step 8: Run publisher/factory tests and commit**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py tests/unit_tests/core/workflow/log_publisher/test_factory.py -v
```

Expected: PASS.

Commit:

```bash
git add api/configs/feature/__init__.py api/core/workflow/log_publisher api/tests/unit_tests/core/workflow/log_publisher
git commit -m "fix: reuse ActiveMQ workflow log publisher connections"
```

- [ ] **Step 9: Run the original pressure-regression scenario**

With Dify API configured for async workflow node logs and the Go consumer running, set the pressure-test target variables and run two consecutive production workflow pressure rounds:

```bash
export DIFY_WORKFLOW_API_TOKEN='set-to-the-test-workflow-api-token'
export DIFY_WORKFLOW_RUN_URL='http://localhost:5001/v1/workflows/run'
test -f payload.json
hey -n 1000 -c 50 -m POST -H 'Content-Type: application/json' -H "Authorization: Bearer ${DIFY_WORKFLOW_API_TOKEN}" -d @payload.json "${DIFY_WORKFLOW_RUN_URL}"
hey -n 1000 -c 50 -m POST -H 'Content-Type: application/json' -H "Authorization: Bearer ${DIFY_WORKFLOW_API_TOKEN}" -d @payload.json "${DIFY_WORKFLOW_RUN_URL}"
```

Expected after the second round:

```text
workflow_runs count for second round == 1000
ActiveMQ enqueueCount increased during second round
Go consumer logs show second-round receive/batch/write/ack activity
workflow_runs with corresponding workflow_node_executions == 1000 after consumer drain
Dify API logs do not contain repeated NotConnectedException publish failures
```

If a small number of publish failures remain, fail-open behavior is still correct, but this task is not complete until the connection lifecycle failure mode no longer causes an entire second pressure round to publish zero node execution events.

## Task 9: Final Verification

**Files:**
- No code changes expected unless verification finds issues.

- [ ] **Step 1: Run focused unit tests**

```bash
cd api && uv run pytest \
  tests/unit_tests/core/workflow/log_publisher \
  tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/core/workflow/test_workflow_cycle_manager.py \
  tests/unit_tests/core/ops/test_workflow_trace_snapshots.py \
  tests/unit_tests/core/ops/test_arize_phoenix_trace.py \
  tests/unit_tests/tasks/test_ops_trace_task.py \
  -v
```

Expected: PASS. Workflow execution repository tests should still pass without async behavior because `workflow_runs` remain synchronous.

- [ ] **Step 2: Run import smoke test with async disabled**

```bash
cd api && uv run python - <<'PY'
from core.repositories.sqlalchemy_workflow_execution_repository import SQLAlchemyWorkflowExecutionRepository
from core.repositories.sqlalchemy_workflow_node_execution_repository import SQLAlchemyWorkflowNodeExecutionRepository
from core.workflow.log_publisher.factory import create_workflow_log_publisher
print('ok')
PY
```

Expected: prints `ok`; must not require a running ActiveMQ broker or import `stomp` unless publisher connection is used.

- [ ] **Step 3: Commit verification fixes if needed**

```bash
git status --short
git add api/configs/feature/__init__.py api/core/workflow/log_publisher api/core/repositories/sqlalchemy_workflow_node_execution_repository.py api/core/workflow api/core/ops api/tasks api/tests/unit_tests
git commit -m "fix: stabilize async workflow node log implementation"
```

Only run the `git add` and `git commit` commands when verification produced actual code or test fixes. If no fixes were needed, do not create an empty commit.

---

## Follow-up Task: Fix Nested Workflow-as-Tool DB Session Lifetime

**Status:** Documented after load-test investigation. This is not an ActiveMQ producer/consumer bug, but it appears after ActiveMQ optimizations increase request throughput.

**Observed failure:** Nested workflow pressure tests can hit SQLAlchemy pool exhaustion:

```text
sqlalchemy.exc.TimeoutError: QueuePool limit of size 20 overflow 0 reached, connection timed out, timeout 30.00
```

**Live DB evidence:** During the failing nested workflow run, PostgreSQL showed many `idle in transaction` connections:

```text
state                | count
---------------------+------
idle in transaction  | 52
idle                 | 12
active               | 6
```

The dominant last query was from workflow-as-tool provider lookup:

```text
idle in transaction | SELECT tool_workflow_providers... | 32
```

Other observed `idle in transaction` queries included `SELECT apps...`, `SELECT workflows...`, `SELECT accounts...`, and `SELECT workflow_runs...`, but `tool_workflow_providers` was the main cluster.

**Likely path:**

```text
api/core/workflow/nodes/tool/tool_node.py
  -> ToolManager.get_workflow_tool_runtime(...)
     -> db.session.query(WorkflowToolProvider) ...
     -> WorkflowToolProviderController.from_db(...)
     -> WorkflowToolProviderController._get_db_provider_tool(...)
  -> ToolEngine.generic_invoke(...)
     -> WorkflowTool._invoke(...)
        -> WorkflowTool._get_app(...)
        -> WorkflowTool._get_workflow(...)
        -> child WorkflowAppGenerator.generate(...)
```

**Files to investigate and likely modify:**

- `api/core/tools/tool_manager.py`
- `api/core/tools/workflow_as_tool/provider.py`
- `api/core/tools/workflow_as_tool/tool.py`
- Tests under `api/tests/unit_tests/core/tools/` or `api/tests/unit_tests/core/workflow/nodes/tool/`

**Design direction:** End read-only DB transactions before invoking the child workflow. Prefer explicit short-lived SQLAlchemy sessions:

```python
from sqlalchemy.orm import Session
from extensions.ext_database import db

with Session(db.engine, expire_on_commit=False) as session:
    row = session.query(...).filter(...).first()
    # eagerly access fields/relationships needed after the session closes
    session.expunge(row)
```

Avoid holding Flask-SQLAlchemy global `db.session` transactions across child workflow execution waits. If using `db.session` is unavoidable in a small patch, explicitly `rollback()`/`close()` after the lookup and before `WorkflowTool._invoke()` calls `WorkflowAppGenerator.generate(...)`, but verify no returned ORM object triggers lazy loads afterward.

**Regression test target:** A nested workflow-as-tool run should not leave provider/app/workflow lookup transactions open while waiting for the child workflow. Unit tests should assert session cleanup around `WorkflowTool._invoke()` / provider lookup, and integration load tests should verify `idle in transaction` does not grow with `SELECT tool_workflow_providers...` during nested workflow pressure tests.

---

## Self-Review Notes

Spec coverage:
- ActiveMQ publisher abstraction: Tasks 1-2.
- ActiveMQ process-level singleton, bounded reconnect retry, warm-up, pool lifecycle, and pressure-regression validation: Task 8.
- Node execution async save with fail-open and cache: Task 3.
- Workflow runs remain synchronous: explicitly out of async tasks; workflow execution repository tests remain in verification.
- Debugging sync routing and backward-compatible node constructor defaults: Tasks 3-4.
- Running node failure completion from cache: Task 3.
- Tracing node snapshots including Arize/Phoenix: Tasks 5-6.
- Eventual consistency and consumer contracts: encoded in node event payloads and preserved in spec; consumer is out of repo scope.
- Security/config docs: Task 7.
- Nested workflow-as-tool SQLAlchemy `idle in transaction` / QueuePool exhaustion is documented as a follow-up task because it is a separate DB session lifetime issue exposed by higher throughput.

No placeholders remain; implementation tasks include exact file paths, concrete code shapes, commands, expected results, and commit points.
