# Async Workflow Log ActiveMQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish non-debugging workflow run and node execution logs to ActiveMQ asynchronously while preserving debugging sync writes, runtime repository semantics, and complete tracing.

**Architecture:** Add a workflow-log publisher abstraction with an ActiveMQ STOMP implementation and explicit `WorkflowLogWriteMode`. Repositories keep existing sync DB behavior by default, but in async mode publish JSON-safe upsert events while maintaining in-memory caches for runtime reads. Tracing receives JSON-safe runtime snapshots so all providers, including Arize/Phoenix, do not depend on async DB persistence.

**Tech Stack:** Python, SQLAlchemy, Pydantic, Flask/Celery, ActiveMQ STOMP via optional `stomp.py`, pytest, unittest.mock.

---

## File Structure

Create:
- `api/core/workflow/log_publisher/__init__.py` — exports publisher types and factory.
- `api/core/workflow/log_publisher/entities.py` — JSON-safe event envelope, write mode enum, payload helpers, trace snapshot DTOs.
- `api/core/workflow/log_publisher/publisher.py` — `WorkflowLogPublisher` protocol and no-op implementation.
- `api/core/workflow/log_publisher/activemq_publisher.py` — thread-safe lazy STOMP publisher.
- `api/core/workflow/log_publisher/factory.py` — config-driven publisher creation.
- `api/tests/unit_tests/core/workflow/log_publisher/test_entities.py` — payload/snapshot serialization tests.
- `api/tests/unit_tests/core/workflow/log_publisher/test_activemq_publisher.py` — ActiveMQ publisher behavior tests.
- `api/tests/unit_tests/core/workflow/log_publisher/test_factory.py` — config/factory tests.

Modify:
- `api/configs/feature/__init__.py` — add workflow log async/ActiveMQ config.
- `api/pyproject.toml` — add optional STOMP dependency if project policy requires declared deps.
- `api/core/repositories/sqlalchemy_workflow_execution_repository.py` — add write mode/publisher and async save path preserving cache.
- `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py` — add write mode/publisher, async save path, cached running lookup and snapshot accessor.
- `api/core/workflow/repositories/workflow_node_execution_repository.py` — add optional snapshot/get cached methods only if needed by `WorkflowCycleManager` typing.
- `api/core/workflow/workflow_cycle_manager.py` — pass final workflow/node snapshots to trace tasks.
- `api/core/ops/entities/trace_entity.py` — add JSON-safe workflow/node trace snapshot fields to `WorkflowTraceInfo`.
- `api/core/ops/ops_trace_manager.py` — build `WorkflowTraceInfo` from snapshots when present.
- `api/tasks/ops_trace_task.py` — stop forcing snapshot workflow data back into `WorkflowRun` when snapshot mode is used.
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
- Existing tests:
  - `api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py`
  - `api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py`
  - `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
  - add/modify trace provider tests as needed.

## Task 1: Config and Core Publisher Entities

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


def test_workflow_log_event_is_json_serializable():
    event = WorkflowLogEvent.create(
        event_type=WorkflowLogEventType.WORKFLOW_RUN_UPSERT,
        payload={"id": "run-1", "created_at": datetime(2026, 6, 6, 1, 2, 3)},
    )

    dumped = event.model_dump(mode="json")

    assert dumped["event_type"] == "workflow_run.upsert"
    assert dumped["schema_version"] == 1
    assert dumped["payload"] == {"id": "run-1", "created_at": "2026-06-06T01:02:03Z"}
    assert dumped["event_id"]
    assert dumped["created_at"].endswith("Z")
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
        description="Whether to publish non-debugging workflow logs asynchronously through a message queue.",
        default=False,
    )

    WORKFLOW_LOG_QUEUE_PROVIDER: str = Field(
        description="Queue provider for async workflow logs. This version supports 'activemq'.",
        default="activemq",
    )

    WORKFLOW_LOG_ACTIVEMQ_HOST: str = Field(
        description="ActiveMQ STOMP host for async workflow log publishing.",
        default="localhost",
    )

    WORKFLOW_LOG_ACTIVEMQ_PORT: PositiveInt = Field(
        description="ActiveMQ STOMP port for async workflow log publishing.",
        default=61613,
    )

    WORKFLOW_LOG_ACTIVEMQ_USERNAME: str | None = Field(
        description="ActiveMQ username for async workflow log publishing.",
        default=None,
    )

    WORKFLOW_LOG_ACTIVEMQ_PASSWORD: str | None = Field(
        description="ActiveMQ password for async workflow log publishing.",
        default=None,
    )

    WORKFLOW_LOG_ACTIVEMQ_DESTINATION: str = Field(
        description="ActiveMQ queue destination for workflow log events.",
        default="/queue/dify.workflow.logs",
    )

    WORKFLOW_LOG_PUBLISH_TIMEOUT: PositiveFloat = Field(
        description="Maximum seconds allowed for one workflow log publish attempt.",
        default=0.2,
    )
```

Ensure `PositiveFloat` is imported at the top of the file; it may already be imported. If not, change the pydantic import to include it.

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
    WORKFLOW_RUN_UPSERT = "workflow_run.upsert"
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
```

Create `api/core/workflow/log_publisher/__init__.py`:

```python
from core.workflow.log_publisher.entities import WorkflowLogEvent, WorkflowLogEventType, WorkflowLogWriteMode

__all__ = ["WorkflowLogEvent", "WorkflowLogEventType", "WorkflowLogWriteMode"]
```

- [ ] **Step 5: Run tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/log_publisher/test_entities.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/configs/feature/__init__.py api/core/workflow/log_publisher api/tests/unit_tests/core/workflow/log_publisher/test_entities.py
git commit -m "feat: add workflow log publisher entities"
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
        event_type=WorkflowLogEventType.WORKFLOW_RUN_UPSERT,
        payload={"id": "run-1"},
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

- [ ] **Step 3: Implement protocol and no-op publisher**

Create `api/core/workflow/log_publisher/publisher.py`:

```python
from __future__ import annotations

from typing import Protocol

from core.workflow.log_publisher.entities import WorkflowLogEvent


class WorkflowLogPublisher(Protocol):
    def publish(self, event: WorkflowLogEvent) -> None: ...


class NoopWorkflowLogPublisher:
    def publish(self, event: WorkflowLogEvent) -> None:
        return None
```

- [ ] **Step 4: Implement ActiveMQ publisher**

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
```

- [ ] **Step 5: Implement factory**

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

Update `api/core/workflow/log_publisher/__init__.py`:

```python
from core.workflow.log_publisher.entities import WorkflowLogEvent, WorkflowLogEventType, WorkflowLogWriteMode
from core.workflow.log_publisher.factory import create_workflow_log_publisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher

__all__ = [
    "NoopWorkflowLogPublisher",
    "WorkflowLogEvent",
    "WorkflowLogEventType",
    "WorkflowLogPublisher",
    "WorkflowLogWriteMode",
    "create_workflow_log_publisher",
]
```

- [ ] **Step 6: Run tests**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/log_publisher -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add api/core/workflow/log_publisher api/tests/unit_tests/core/workflow/log_publisher
git commit -m "feat: add ActiveMQ workflow log publisher"
```

## Task 3: Workflow Run Repository Async Save

**Files:**
- Modify: `api/core/repositories/sqlalchemy_workflow_execution_repository.py`
- Test: `api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py`

- [ ] **Step 1: Add failing tests**

Append to `api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py`:

```python
from core.workflow.log_publisher.entities import WorkflowLogEventType, WorkflowLogWriteMode


def test_async_save_publishes_workflow_run_and_updates_cache(repository, mocker):
    publisher = mocker.Mock()
    repository._write_mode = WorkflowLogWriteMode.ASYNC
    repository._workflow_log_publisher = publisher
    execution = _create_workflow_execution()

    repository.save(execution)

    publisher.publish.assert_called_once()
    event = publisher.publish.call_args.args[0]
    assert event.event_type == WorkflowLogEventType.WORKFLOW_RUN_UPSERT
    assert event.payload["id"] == execution.id_
    assert repository.get(execution.id_).id_ == execution.id_


def test_async_save_fail_open_still_updates_cache(repository, mocker):
    publisher = mocker.Mock()
    publisher.publish.side_effect = RuntimeError("broker down")
    repository._write_mode = WorkflowLogWriteMode.ASYNC
    repository._workflow_log_publisher = publisher
    execution = _create_workflow_execution()

    repository.save(execution)

    assert repository.get(execution.id_).id_ == execution.id_
```

If this file does not already have `_create_workflow_execution()`, add this helper near existing fixtures using the actual existing `WorkflowExecution` constructor pattern in the file:

```python
def _create_workflow_execution():
    return WorkflowExecution.new(
        id_="run-1",
        workflow_id="workflow-1",
        workflow_type=WorkflowType.WORKFLOW,
        workflow_version="1",
        graph={},
        inputs={"query": "hello"},
        started_at=datetime(2026, 6, 6, 1, 2, 3),
    )
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd api && uv run pytest tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py -v
```

Expected: FAIL because constructor/write mode/publisher fields do not exist or async save still writes DB.

- [ ] **Step 3: Implement async save path**

In `api/core/repositories/sqlalchemy_workflow_execution_repository.py`:

1. Add imports:

```python
from core.workflow.log_publisher.entities import WorkflowLogEvent, WorkflowLogEventType, WorkflowLogWriteMode
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher
```

2. Extend constructor with defaults:

```python
        write_mode: WorkflowLogWriteMode = WorkflowLogWriteMode.SYNC,
        workflow_log_publisher: WorkflowLogPublisher | None = None,
```

3. Set fields in constructor:

```python
        self._write_mode = write_mode
        self._workflow_log_publisher = workflow_log_publisher or NoopWorkflowLogPublisher()
```

4. Refactor `save()` so cache is updated for both paths:

```python
        db_model = self._to_db_model(execution)

        if self._write_mode == WorkflowLogWriteMode.ASYNC:
            try:
                self._workflow_log_publisher.publish(
                    WorkflowLogEvent.create(
                        event_type=WorkflowLogEventType.WORKFLOW_RUN_UPSERT,
                        payload=db_model.to_dict() if hasattr(db_model, "to_dict") else self._workflow_run_payload(db_model),
                    )
                )
            except Exception:
                logger.warning("Failed to publish workflow run log event", exc_info=True, extra={"workflow_run_id": db_model.id})
            self._execution_cache[db_model.id] = db_model
            return
```

5. Add helper if `WorkflowRun.to_dict()` is not suitable for DB upsert fields:

```python
    def _workflow_run_payload(self, db_model: WorkflowRun) -> dict:
        return {
            "id": db_model.id,
            "tenant_id": db_model.tenant_id,
            "app_id": db_model.app_id,
            "workflow_id": db_model.workflow_id,
            "triggered_from": db_model.triggered_from,
            "type": db_model.type,
            "version": db_model.version,
            "graph": db_model.graph_dict,
            "inputs": db_model.inputs_dict,
            "outputs": db_model.outputs_dict,
            "status": db_model.status,
            "error": db_model.error,
            "elapsed_time": db_model.elapsed_time,
            "total_tokens": db_model.total_tokens,
            "total_steps": db_model.total_steps,
            "exceptions_count": db_model.exceptions_count,
            "created_by_role": db_model.created_by_role,
            "created_by": db_model.created_by,
            "created_at": db_model.created_at,
            "finished_at": db_model.finished_at,
        }
```

Keep the existing synchronous `execute_with_db_retry()` path unchanged after the async branch.

- [ ] **Step 4: Run repository tests**

```bash
cd api && uv run pytest tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/core/repositories/sqlalchemy_workflow_execution_repository.py api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py
git commit -m "feat: publish workflow run logs asynchronously"
```

## Task 4: Node Execution Repository Async Save and Cache Semantics

**Files:**
- Modify: `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`
- Test: `api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py`

- [ ] **Step 1: Add failing tests**

Append to `api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py`:

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


def test_async_get_running_executions_returns_cached_running_nodes(repository, mocker):
    repository._write_mode = WorkflowLogWriteMode.ASYNC
    repository._workflow_log_publisher = mocker.Mock()
    running = _create_domain_node_execution(status=WorkflowNodeExecutionStatus.RUNNING)
    succeeded = _create_domain_node_execution(
        id="record-2",
        node_execution_id="node-exec-2",
        status=WorkflowNodeExecutionStatus.SUCCEEDED,
    )

    repository.save(running)
    repository.save(succeeded)

    results = repository.get_running_executions(running.workflow_execution_id)

    assert [item.node_execution_id for item in results] == [running.node_execution_id]
```

Add helper if missing:

```python
def _create_domain_node_execution(
    *,
    id="record-1",
    node_execution_id="node-exec-1",
    status=WorkflowNodeExecutionStatus.RUNNING,
):
    return WorkflowNodeExecution(
        id=id,
        node_execution_id=node_execution_id,
        workflow_id="workflow-1",
        workflow_execution_id="run-1",
        index=1,
        predecessor_node_id="start",
        node_id="llm",
        node_type=NodeType.LLM,
        title="LLM",
        inputs={"query": "hello"},
        process_data={"prompts": []},
        outputs={},
        status=status,
        metadata={},
        created_at=datetime(2026, 6, 6, 1, 2, 3),
    )
```

- [ ] **Step 2: Run tests to verify failure**

```bash
cd api && uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -v
```

Expected: FAIL because async path and cached running lookup do not exist.

- [ ] **Step 3: Implement async node save**

In `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`:

1. Add imports:

```python
from core.workflow.log_publisher.entities import WorkflowLogEvent, WorkflowLogEventType, WorkflowLogWriteMode
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher
```

2. Extend constructor with defaults:

```python
        write_mode: WorkflowLogWriteMode = WorkflowLogWriteMode.SYNC,
        workflow_log_publisher: WorkflowLogPublisher | None = None,
```

3. Set fields:

```python
        self._write_mode = write_mode
        self._workflow_log_publisher = workflow_log_publisher or NoopWorkflowLogPublisher()
```

4. Add payload helper:

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

5. In `save()`, after `db_model = self.to_db_model(execution)`, add async branch:

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

- [ ] **Step 4: Implement cached running lookup**

In `get_running_executions()`, before or after DB query, collect cached running nodes:

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

For sync mode, merge cached and DB rows after the existing DB query:

```python
            models_by_key = {model.node_execution_id or model.id: model for model in cached_models}
            for model in db_models:
                models_by_key[model.node_execution_id or model.id] = model
            return [self._to_domain_model(model) for model in models_by_key.values()]
```

- [ ] **Step 5: Add snapshot accessor**

Add method:

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
```

Import `datetime` if needed.

- [ ] **Step 6: Run tests**

```bash
cd api && uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -v
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add api/core/repositories/sqlalchemy_workflow_node_execution_repository.py api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py
git commit -m "feat: publish workflow node logs asynchronously"
```

## Task 5: Wire Async Mode in App Generators

**Files:**
- Modify: `api/core/app/apps/workflow/app_generator.py`
- Modify: `api/core/app/apps/advanced_chat/app_generator.py`
- Test: add targeted unit test if existing app generator tests are available; otherwise run repository and workflow cycle tests.

- [ ] **Step 1: Add helper function in both app generators**

In both files, add imports:

```python
from configs import dify_config
from core.workflow.log_publisher import WorkflowLogWriteMode, create_workflow_log_publisher
```

Add local helper near repository creation code:

```python
def _workflow_log_write_mode_for_invoke(invoke_from: InvokeFrom) -> WorkflowLogWriteMode:
    if invoke_from == InvokeFrom.DEBUGGER:
        return WorkflowLogWriteMode.SYNC
    if not dify_config.WORKFLOW_LOG_ASYNC_ENABLED:
        return WorkflowLogWriteMode.SYNC
    return WorkflowLogWriteMode.ASYNC
```

If top-level helper placement causes import cycles, define it as a private static method on the generator class instead.

- [ ] **Step 2: Pass write mode and publisher for full workflow runs**

At each full workflow repository construction site, before constructing repositories:

```python
        workflow_log_write_mode = _workflow_log_write_mode_for_invoke(invoke_from)
        workflow_log_publisher = create_workflow_log_publisher(dify_config)
```

Pass to both repositories:

```python
            write_mode=workflow_log_write_mode,
            workflow_log_publisher=workflow_log_publisher,
```

Single-step debugging paths must either omit these parameters or pass:

```python
            write_mode=WorkflowLogWriteMode.SYNC,
```

Do not enable async for `single_iteration_generate`, `single_loop_generate`, or `workflow_service.run_free_workflow_node` single-step paths.

- [ ] **Step 3: Run focused tests**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/test_workflow_cycle_manager.py tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -v
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add api/core/app/apps/workflow/app_generator.py api/core/app/apps/advanced_chat/app_generator.py
git commit -m "feat: enable async workflow log routing"
```

## Task 6: JSON-Safe Trace Snapshot DTOs and TraceTask Plumbing

**Files:**
- Modify: `api/core/workflow/log_publisher/entities.py`
- Modify: `api/core/workflow/workflow_cycle_manager.py`
- Modify: `api/core/ops/entities/trace_entity.py`
- Modify: `api/core/ops/ops_trace_manager.py`
- Modify: `api/tasks/ops_trace_task.py`
- Test: `api/tests/unit_tests/core/ops/test_trace_context.py` or new `api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py`

- [ ] **Step 1: Write failing snapshot tests**

Create `api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py`:

```python
from datetime import datetime

from core.ops.entities.trace_entity import WorkflowTraceInfo
from core.workflow.log_publisher.entities import NodeExecutionTraceSnapshot, WorkflowRunTraceSnapshot


def test_trace_snapshots_are_json_safe_through_workflow_trace_info():
    workflow_snapshot = WorkflowRunTraceSnapshot(
        id="run-1",
        tenant_id="tenant-1",
        app_id="app-1",
        workflow_id="workflow-1",
        triggered_from="app-run",
        type="workflow",
        version="1",
        graph={},
        inputs={"query": "hello"},
        outputs={"answer": "world"},
        status="succeeded",
        error=None,
        elapsed_time=1.2,
        total_tokens=10,
        total_steps=2,
        exceptions_count=0,
        created_at=datetime(2026, 6, 6, 1, 2, 3),
        finished_at=datetime(2026, 6, 6, 1, 2, 4),
    )
    node_snapshot = NodeExecutionTraceSnapshot(
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
        workflow_snapshot=workflow_snapshot.model_dump(mode="json"),
        node_execution_snapshots=[node_snapshot.model_dump(mode="json")],
    )

    restored = WorkflowTraceInfo.model_validate_json(trace_info.model_dump_json())

    assert restored.workflow_snapshot["created_at"] == "2026-06-06T01:02:03Z"
    assert restored.node_execution_snapshots[0]["metadata"] == {"total_tokens": 10}
```

- [ ] **Step 2: Run test to verify failure**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_workflow_trace_snapshots.py -v
```

Expected: FAIL because snapshot DTOs/fields do not exist.

- [ ] **Step 3: Implement snapshot DTOs**

In `api/core/workflow/log_publisher/entities.py`, add:

```python
class WorkflowRunTraceSnapshot(BaseModel):
    id: str
    tenant_id: str
    app_id: str | None = None
    workflow_id: str
    triggered_from: str
    type: str
    version: str
    graph: dict[str, Any] | None = None
    inputs: dict[str, Any] | None = None
    outputs: dict[str, Any] | None = None
    status: str
    error: str | None = None
    elapsed_time: int | float
    total_tokens: int
    total_steps: int
    exceptions_count: int
    created_at: datetime
    finished_at: datetime | None = None

    @field_serializer("created_at", "finished_at")
    def serialize_datetime_fields(self, value: datetime | None) -> str | None:
        return _serialize_datetime(value) if value else None


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

- [ ] **Step 4: Add fields to WorkflowTraceInfo**

In `api/core/ops/entities/trace_entity.py`, add to `WorkflowTraceInfo`:

```python
    workflow_snapshot: Optional[dict[str, Any]] = None
    node_execution_snapshots: list[dict[str, Any]] = []
```

- [ ] **Step 5: Build snapshots in TraceTask**

In `api/core/ops/ops_trace_manager.py`, extend `TraceTask.__init__` to accept:

```python
        workflow_snapshot: dict[str, Any] | None = None,
        node_execution_snapshots: list[dict[str, Any]] | None = None,
```

Set:

```python
        self.workflow_snapshot = workflow_snapshot
        self.node_execution_snapshots = node_execution_snapshots or []
```

In `workflow_trace()`, before DB query, if `self.workflow_snapshot` exists, build `WorkflowTraceInfo` from it. Use this exact mapping:

```python
        if self.workflow_snapshot:
            workflow_run = self.workflow_snapshot
            workflow_run_inputs = workflow_run.get("inputs") or {}
            workflow_run_outputs = workflow_run.get("outputs") or {}
            metadata = {
                "workflow_id": workflow_run.get("workflow_id"),
                "conversation_id": conversation_id,
                "workflow_run_id": workflow_run.get("id"),
                "tenant_id": workflow_run.get("tenant_id"),
                "elapsed_time": workflow_run.get("elapsed_time") or 0,
                "status": workflow_run.get("status"),
                "version": workflow_run.get("version"),
                "total_tokens": workflow_run.get("total_tokens") or 0,
                "file_list": workflow_run_inputs.get("sys.file") or [],
                "triggered_from": workflow_run.get("triggered_from"),
                "user_id": user_id,
                "app_id": workflow_run.get("app_id"),
            }
            metadata.update(self.kwargs.get("metadata", {}) or {})
            return WorkflowTraceInfo(
                workflow_data=workflow_run,
                conversation_id=conversation_id,
                workflow_id=workflow_run.get("workflow_id") or "",
                tenant_id=workflow_run.get("tenant_id") or "",
                workflow_run_id=workflow_run.get("id") or workflow_run_id,
                workflow_run_elapsed_time=workflow_run.get("elapsed_time") or 0,
                workflow_run_status=workflow_run.get("status") or "",
                workflow_run_inputs=workflow_run_inputs,
                workflow_run_outputs=workflow_run_outputs,
                workflow_run_version=workflow_run.get("version") or "",
                error=workflow_run.get("error") or "",
                total_tokens=workflow_run.get("total_tokens") or 0,
                file_list=workflow_run_inputs.get("sys.file") or [],
                query=workflow_run_inputs.get("query") or workflow_run_inputs.get("sys.query") or "",
                metadata=metadata,
                workflow_app_log_id=None,
                message_id=None,
                start_time=workflow_run.get("created_at"),
                end_time=workflow_run.get("finished_at"),
                workflow_snapshot=workflow_run,
                node_execution_snapshots=self.node_execution_snapshots,
            )
```

Adjust `start_time`/`end_time` if Pydantic requires datetime: parse ISO strings with `datetime.fromisoformat(value.replace("Z", ""))` in a helper.

- [ ] **Step 6: Avoid reconstructing snapshot workflow data as WorkflowRun**

In `api/tasks/ops_trace_task.py`, change:

```python
    if trace_info.get("workflow_data"):
        trace_info["workflow_data"] = WorkflowRun.from_dict(data=trace_info["workflow_data"])
```

to:

```python
    if trace_info.get("workflow_data") and not trace_info.get("workflow_snapshot"):
        trace_info["workflow_data"] = WorkflowRun.from_dict(data=trace_info["workflow_data"])
```

- [ ] **Step 7: Run snapshot tests**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_workflow_trace_snapshots.py tests/unit_tests/tasks/test_ops_trace_task.py -v
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add api/core/workflow/log_publisher/entities.py api/core/ops/entities/trace_entity.py api/core/ops/ops_trace_manager.py api/tasks/ops_trace_task.py api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py
git commit -m "feat: add JSON-safe workflow trace snapshots"
```

## Task 7: Collect Trace Snapshots from WorkflowCycleManager

**Files:**
- Modify: `api/core/workflow/workflow_cycle_manager.py`
- Modify: `api/core/repositories/sqlalchemy_workflow_execution_repository.py`
- Modify: `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`
- Test: `api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py`

- [ ] **Step 1: Write failing WorkflowCycleManager trace test**

Add to `api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py` near success trace tests:

```python
def test_workflow_success_trace_task_receives_json_safe_snapshots(workflow_cycle_manager, mocker):
    trace_manager = mocker.Mock()
    workflow_execution = workflow_cycle_manager.handle_workflow_run_start()
    node_event = _create_node_started_event(node_execution_id="node-exec-1")
    workflow_cycle_manager.handle_node_execution_start(workflow_execution_id=workflow_execution.id_, event=node_event)
    success_event = _create_node_succeeded_event(node_execution_id="node-exec-1")
    workflow_cycle_manager.handle_workflow_node_execution_success(event=success_event)

    workflow_cycle_manager.handle_workflow_run_success(
        workflow_run_id=workflow_execution.id_,
        total_tokens=10,
        total_steps=1,
        outputs={"answer": "world"},
        trace_manager=trace_manager,
    )

    trace_task = trace_manager.add_trace_task.call_args.args[0]
    assert trace_task.workflow_snapshot["id"] == workflow_execution.id_
    assert trace_task.workflow_snapshot["status"] == "succeeded"
    assert trace_task.node_execution_snapshots[0]["node_execution_id"] == "node-exec-1"
```

Use existing event helper names in the file. If helpers differ, adapt names but keep assertions.

- [ ] **Step 2: Run test to verify failure**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/test_workflow_cycle_manager.py::test_workflow_success_trace_task_receives_json_safe_snapshots -v
```

Expected: FAIL because snapshots are not passed.

- [ ] **Step 3: Add repository snapshot methods**

In workflow execution repository, add:

```python
    def to_trace_snapshot(self, execution: WorkflowExecution) -> dict:
        db_model = self._to_db_model(execution)
        return WorkflowRunTraceSnapshot(**self._workflow_run_payload(db_model)).model_dump(mode="json")
```

Import `WorkflowRunTraceSnapshot`.

In node execution repository, add:

```python
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

Import `NodeExecutionTraceSnapshot`.

- [ ] **Step 4: Pass snapshots to trace tasks**

In `WorkflowCycleManager`, add helper:

```python
    def _workflow_trace_snapshots(self, workflow_execution: WorkflowExecution) -> tuple[dict | None, list[dict]]:
        workflow_snapshot = None
        if hasattr(self._workflow_execution_repository, "to_trace_snapshot"):
            workflow_snapshot = self._workflow_execution_repository.to_trace_snapshot(workflow_execution)

        node_snapshots = []
        if hasattr(self._workflow_node_execution_repository, "get_cached_executions_by_workflow_run"):
            node_executions = self._workflow_node_execution_repository.get_cached_executions_by_workflow_run(
                workflow_execution.id_
            )
            if hasattr(self._workflow_node_execution_repository, "to_trace_snapshot"):
                node_snapshots = [
                    self._workflow_node_execution_repository.to_trace_snapshot(node_execution)
                    for node_execution in node_executions
                ]
        return workflow_snapshot, node_snapshots
```

Before every workflow trace `TraceTask(...)` creation in success, partial success, and failed handlers, compute after mutating workflow execution and after running-node terminal updates:

```python
            workflow_snapshot, node_execution_snapshots = self._workflow_trace_snapshots(workflow_execution)
```

Pass to `TraceTask`:

```python
                    workflow_snapshot=workflow_snapshot,
                    node_execution_snapshots=node_execution_snapshots,
```

- [ ] **Step 5: Run tests**

```bash
cd api && uv run pytest tests/unit_tests/core/workflow/test_workflow_cycle_manager.py tests/unit_tests/core/ops/test_workflow_trace_snapshots.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/core/workflow/workflow_cycle_manager.py api/core/repositories/sqlalchemy_workflow_execution_repository.py api/core/repositories/sqlalchemy_workflow_node_execution_repository.py api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py
git commit -m "feat: pass workflow trace snapshots"
```

## Task 8: Update Trace Providers to Prefer Snapshots

**Files:**
- Modify all workflow trace providers listed in File Structure.
- Test: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
- Add tests for one domain-style provider if existing tests are easy to extend.

- [ ] **Step 1: Write failing Arize/Phoenix snapshot test**

In `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`, add:

```python
def test_phoenix_workflow_trace_uses_node_snapshots_when_db_empty(mocker, workflow_trace_info):
    from core.ops.arize_phoenix_trace.arize_phoenix_trace import ArizePhoenixTrace

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
    trace = ArizePhoenixTrace(mocker.Mock())
    mocker.patch.object(trace, "_get_workflow_nodes", return_value=[])
    add_span = mocker.patch.object(trace, "_add_span")

    trace.workflow_trace(workflow_trace_info)

    assert add_span.called
```

Adjust constructor/mocking to match existing test patterns in the file. The assertion should prove DB `_get_workflow_nodes()` returning empty does not prevent node span generation.

- [ ] **Step 2: Run test to verify failure**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_phoenix_workflow_trace_uses_node_snapshots_when_db_empty -v
```

Expected: FAIL because provider ignores snapshots.

- [ ] **Step 3: Add provider-neutral snapshot adapter**

Create helper in `api/core/ops/entities/trace_entity.py` or a new small file `api/core/ops/workflow_trace_snapshots.py`:

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

For Arize/Phoenix DB-row-like code that expects tuple attributes `execution_metadata`, `process_data`, `inputs`, etc., use this adapter.

- [ ] **Step 4: Update providers**

In Langfuse/Langsmith/Weave/Opik/Aliyun, replace DB-only node loading with:

```python
from core.ops.workflow_trace_snapshots import workflow_node_snapshot_to_domain_like


def _workflow_node_executions_from_trace_info(trace_info):
    if getattr(trace_info, "node_execution_snapshots", None):
        return [workflow_node_snapshot_to_domain_like(snapshot) for snapshot in trace_info.node_execution_snapshots]
    return None
```

Then in `workflow_trace()`:

```python
        workflow_node_executions = _workflow_node_executions_from_trace_info(trace_info)
        if workflow_node_executions is None:
            workflow_node_executions = workflow_node_execution_repository.get_by_workflow_run(
                workflow_run_id=trace_info.workflow_run_id
            )
```

In Arize/Phoenix, change:

```python
            workflow_nodes = list(self._get_workflow_nodes(trace_info.workflow_run_id))
```

to:

```python
            if trace_info.node_execution_snapshots:
                workflow_nodes = [
                    workflow_node_snapshot_to_domain_like(snapshot)
                    for snapshot in trace_info.node_execution_snapshots
                ]
            else:
                workflow_nodes = list(self._get_workflow_nodes(trace_info.workflow_run_id))
```

If Arize/Phoenix helper functions parse JSON strings from DB rows, make them accept already-dict values:

```python
def _json_loads_if_string(value):
    if isinstance(value, str):
        return json.loads(value) if value else {}
    return value or {}
```

Use it where provider reads `inputs`, `outputs`, `process_data`, or `execution_metadata`.

- [ ] **Step 5: Run trace tests**

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_arize_phoenix_trace.py tests/unit_tests/core/ops/test_workflow_trace_snapshots.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/core/ops api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py
git commit -m "feat: use snapshots for workflow tracing"
```

## Task 9: Dependency Declaration and Documentation/Environment Notes

**Files:**
- Modify: `api/pyproject.toml`
- Modify: `.env.example` or deployment env example files if present.
- Modify: `docs/superpowers/specs/2026-06-06-async-workflow-log-activemq-design.md` only if implementation discovers a concrete deviation.

- [ ] **Step 1: Check dependency policy**

Run:

```bash
cd api && rg -n "\[project.optional-dependencies\]|stomp|celery" pyproject.toml
```

Expected: See existing dependency layout.

- [ ] **Step 2: Add `stomp.py` dependency**

If project uses mandatory dependencies only, add `stomp.py` to the main dependencies in `api/pyproject.toml`:

```toml
"stomp.py>=8.1.0",
```

If project uses optional dependencies, add:

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
# Async workflow log publishing. Disabled by default.
WORKFLOW_LOG_ASYNC_ENABLED=false
WORKFLOW_LOG_QUEUE_PROVIDER=activemq
WORKFLOW_LOG_ACTIVEMQ_HOST=localhost
WORKFLOW_LOG_ACTIVEMQ_PORT=61613
WORKFLOW_LOG_ACTIVEMQ_USERNAME=
WORKFLOW_LOG_ACTIVEMQ_PASSWORD=
WORKFLOW_LOG_ACTIVEMQ_DESTINATION=/queue/dify.workflow.logs
WORKFLOW_LOG_PUBLISH_TIMEOUT=0.2
```

- [ ] **Step 4: Run dependency lock command if required**

If this repo requires lock updates, run the existing command. First inspect `api/README.md` or `Makefile`; likely command:

```bash
cd api && uv lock
```

Expected: `api/uv.lock` updates only for the new dependency.

- [ ] **Step 5: Commit**

```bash
git add api/pyproject.toml api/uv.lock .
git commit -m "chore: document async workflow log configuration"
```

## Task 10: Final Verification

**Files:**
- No code changes expected.

- [ ] **Step 1: Run focused unit tests**

```bash
cd api && uv run pytest \
  tests/unit_tests/core/workflow/log_publisher \
  tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/core/workflow/test_workflow_cycle_manager.py \
  tests/unit_tests/core/ops/test_workflow_trace_snapshots.py \
  tests/unit_tests/core/ops/test_arize_phoenix_trace.py \
  tests/unit_tests/tasks/test_ops_trace_task.py \
  -v
```

Expected: PASS.

- [ ] **Step 2: Run formatting/linting command used by repo**

Inspect `api/pyproject.toml` for lint commands. If ruff is configured, run:

```bash
cd api && uv run ruff check core tests/unit_tests/core/workflow/log_publisher tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py tests/unit_tests/core/ops/test_workflow_trace_snapshots.py
```

Expected: PASS.

- [ ] **Step 3: Run import smoke test with async disabled**

```bash
cd api && uv run python - <<'PY'
from core.repositories.sqlalchemy_workflow_execution_repository import SQLAlchemyWorkflowExecutionRepository
from core.repositories.sqlalchemy_workflow_node_execution_repository import SQLAlchemyWorkflowNodeExecutionRepository
from core.workflow.log_publisher.factory import create_workflow_log_publisher
print('ok')
PY
```

Expected: prints `ok`; must not require a running ActiveMQ broker or import `stomp` unless publisher connection is used.

- [ ] **Step 4: Commit any verification fixes**

If verification required fixes:

```bash
git add <changed-files>
git commit -m "fix: stabilize async workflow log implementation"
```

If no fixes were needed, do not create an empty commit.

---

## Self-Review Notes

Spec coverage:
- ActiveMQ publisher abstraction: Tasks 1-2.
- Async repository save with fail-open and cache: Tasks 3-4.
- Debugging sync routing and backward-compatible constructor defaults: Tasks 3-5.
- Running node failure completion from cache: Task 4.
- Tracing snapshots including Arize/Phoenix: Tasks 6-8.
- Eventual consistency and consumer contracts: encoded in event payloads/tests and preserved in spec; consumer is out of repo scope.
- Security/config docs: Task 9.

No placeholders remain; implementation tasks include exact file paths, concrete code shapes, commands, expected results, and commit points.
