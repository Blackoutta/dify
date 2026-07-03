# ActiveMQ Workflow Node Log Backport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Backport the Dify-side workflow node execution ActiveMQ producer path onto `origin/1.13.3` without pulling unrelated post-1.13.3 refactors.

**Architecture:** Keep the existing 1.13.3 synchronous SQLAlchemy repository as the default. Add a producer-only repository that implements the existing `dify_graph.repositories.workflow_node_execution_repository.WorkflowNodeExecutionRepository` protocol and is selected only for `WORKFLOW_RUN + APP_RUN` when async logging is enabled. The producer keeps a per-instance in-memory read cache and `state_version` map under a lock, then publishes the event outside the lock.

**Tech Stack:** Python, Flask config via Pydantic settings, SQLAlchemy/Alembic, stdlib `socket` STOMP publisher, pytest.

---

## Scope

This plan is Dify-side only. Do not port consumer code, offload support, `graphon` package refactors, docker env restructuring, or default-on ActiveMQ behavior.

Execution must start from `origin/1.13.3`, not from current `HEAD`.

```bash
git fetch origin --tags
git switch --detach 1.13.3
git switch -c blackoutta/backport-activemq-workflow-node-logs-1.13.3
```

Expected: a new branch at the `1.13.3` tag. Do not run these commands until implementation begins.

## File Structure

- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/models/workflow.py`
  - Adds nullable `workflow_node_executions.state_version` ORM field.
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/migrations/versions/2026_07_03_1200-a1b2c3d4e5f6_add_workflow_node_execution_state_version.py`
  - Adds and removes nullable bigint `state_version`.
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/configs/feature/__init__.py`
  - Adds disabled-by-default ActiveMQ workflow log producer config.
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/.env.example`
  - Documents the new API env vars only.
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/repositories/workflow_node_execution_activemq_repository.py`
  - Producer-only repository and minimal STOMP publisher.
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/repositories/factory.py`
  - Adds conservative async routing.
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/app/apps/workflow/app_generator.py`
  - Passes `workflow_triggered_from` for normal workflow runs.
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/app/apps/advanced_chat/app_generator.py`
  - Passes `workflow_triggered_from` for normal advanced-chat workflow runs.
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/app/apps/pipeline/pipeline_generator.py`
  - Passes RAG workflow trigger source explicitly so it stays synchronous.
- Test: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/configs/test_workflow_log_config.py`
- Test: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/migrations/test_workflow_node_execution_state_version.py`
- Test: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/models/test_workflow_models.py`
- Test: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py`
- Test: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py`

### Task 1: Schema And Config Tests

**Files:**
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/configs/test_workflow_log_config.py`
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/migrations/test_workflow_node_execution_state_version.py`
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/models/test_workflow_models.py`

- [ ] **Step 1: Add config default test**

```python
from configs.feature import WorkflowLogConfig


def test_async_workflow_log_defaults_are_disabled() -> None:
    config = WorkflowLogConfig()

    assert config.WORKFLOW_LOG_ASYNC_ENABLED is False
    assert config.WORKFLOW_LOG_QUEUE_PROVIDER == "activemq"
    assert config.WORKFLOW_LOG_ACTIVEMQ_HOST == "localhost"
    assert config.WORKFLOW_LOG_ACTIVEMQ_PORT == 61613
    assert config.WORKFLOW_LOG_ACTIVEMQ_USERNAME == ""
    assert config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD == ""
    assert config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION == "/queue/dify.workflow.logs"
    assert config.WORKFLOW_LOG_PUBLISH_TIMEOUT == 0.2
    assert config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES == 1
```

- [ ] **Step 2: Add migration test**

```python
import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

MIGRATION = (
    Path(__file__).parents[3]
    / "migrations"
    / "versions"
    / "2026_07_03_1200-a1b2c3d4e5f6_add_workflow_node_execution_state_version.py"
)


def test_migration_adds_nullable_bigint_state_version(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = importlib.util.spec_from_file_location("state_version_migration", MIGRATION)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class FakeBatch:
        column: sa.Column | None = None

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def add_column(self, column: sa.Column) -> None:
            self.column = column

    batch = FakeBatch()
    monkeypatch.setattr(module.op, "batch_alter_table", lambda *args, **kwargs: batch)

    module.upgrade()

    column = batch.column
    assert column is not None
    assert column.name == "state_version"
    assert isinstance(column.type, sa.BigInteger)
    assert column.nullable is True
```

- [ ] **Step 3: Add ORM nullable field test**

Add this test near existing `WorkflowNodeExecutionModel` tests:

```python
def test_node_execution_state_version_is_nullable(self):
    node_execution = WorkflowNodeExecutionModel(
        tenant_id="tenant-id",
        app_id="app-id",
        workflow_id="workflow-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_run_id="run-id",
        index=1,
        node_id="node-id",
        node_type="llm",
        title="LLM",
        status="running",
        created_by_role=CreatorUserRole.ACCOUNT,
        created_by="user-id",
    )

    assert node_execution.state_version is None
```

- [ ] **Step 4: Run tests and verify they fail**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/configs/test_workflow_log_config.py \
  api/tests/unit_tests/migrations/test_workflow_node_execution_state_version.py \
  api/tests/unit_tests/models/test_workflow_models.py::TestWorkflowNodeExecutionModel::test_node_execution_state_version_is_nullable -v
```

Expected: failures for missing config fields, missing migration file, and missing `state_version`.

### Task 2: Schema And Config Implementation

**Files:**
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/models/workflow.py`
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/migrations/versions/2026_07_03_1200-a1b2c3d4e5f6_add_workflow_node_execution_state_version.py`
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/configs/feature/__init__.py`
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/.env.example`

- [ ] **Step 1: Add ORM field**

In `WorkflowNodeExecutionModel`, after `finished_at`:

```python
state_version: Mapped[int | None] = mapped_column(sa.BigInteger, nullable=True)
```

- [ ] **Step 2: Add migration**

```python
"""add workflow node execution state version

Revision ID: a1b2c3d4e5f6
Revises: 6b5f9f8b1a2c
Create Date: 2026-07-03 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

revision = "a1b2c3d4e5f6"
down_revision = "6b5f9f8b1a2c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("workflow_node_executions", schema=None) as batch_op:
        batch_op.add_column(sa.Column("state_version", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("workflow_node_executions", schema=None) as batch_op:
        batch_op.drop_column("state_version")
```

- [ ] **Step 3: Add config fields**

Append these fields to `WorkflowLogConfig`:

```python
WORKFLOW_LOG_ASYNC_ENABLED: bool = Field(
    default=False,
    description="Publish workflow node execution logs asynchronously instead of writing them in the API path",
)
WORKFLOW_LOG_QUEUE_PROVIDER: str = Field(default="activemq", description="Workflow log async queue provider")
WORKFLOW_LOG_ACTIVEMQ_HOST: str = Field(default="localhost", description="ActiveMQ STOMP host")
WORKFLOW_LOG_ACTIVEMQ_PORT: int = Field(default=61613, description="ActiveMQ STOMP port")
WORKFLOW_LOG_ACTIVEMQ_USERNAME: str = Field(default="", description="ActiveMQ username")
WORKFLOW_LOG_ACTIVEMQ_PASSWORD: str = Field(default="", description="ActiveMQ password")
WORKFLOW_LOG_ACTIVEMQ_DESTINATION: str = Field(
    default="/queue/dify.workflow.logs",
    description="ActiveMQ destination for workflow node execution log events",
)
WORKFLOW_LOG_PUBLISH_TIMEOUT: float = Field(default=0.2, description="Workflow log publish timeout in seconds")
WORKFLOW_LOG_PUBLISH_MAX_RETRIES: int = Field(default=1, description="Workflow log publish retry attempts")
```

- [ ] **Step 4: Document API env only**

Add to `api/.env.example` near workflow log cleanup settings:

```env
# Whether to publish production workflow node execution logs to a queue.
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

- [ ] **Step 5: Run tests and verify they pass**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/configs/test_workflow_log_config.py \
  api/tests/unit_tests/migrations/test_workflow_node_execution_state_version.py \
  api/tests/unit_tests/models/test_workflow_models.py::TestWorkflowNodeExecutionModel::test_node_execution_state_version_is_nullable -v
```

Expected: all selected tests pass.

- [ ] **Step 6: Commit**

```bash
git add api/models/workflow.py \
  api/migrations/versions/2026_07_03_1200-a1b2c3d4e5f6_add_workflow_node_execution_state_version.py \
  api/configs/feature/__init__.py \
  api/.env.example \
  api/tests/unit_tests/configs/test_workflow_log_config.py \
  api/tests/unit_tests/migrations/test_workflow_node_execution_state_version.py \
  api/tests/unit_tests/models/test_workflow_models.py
git commit -m "feat: add workflow node execution state version config"
```

### Task 3: ActiveMQ Producer Repository Tests

**Files:**
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py`

- [ ] **Step 1: Add repository behavior tests**

```python
from collections.abc import Iterator
from datetime import datetime
from threading import Lock
from typing import Any

from core.repositories.workflow_node_execution_activemq_repository import ActiveMQWorkflowNodeExecutionRepository
from dify_graph.entities import WorkflowNodeExecution
from dify_graph.enums import BuiltinNodeTypes, WorkflowNodeExecutionStatus
from models import Account, CreatorUserRole, Tenant, WorkflowNodeExecutionTriggeredFrom


class FakePublisher:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def publish(self, event: dict[str, Any]) -> None:
        self.messages.append(event)


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
            self.locked_during_publish = False

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
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py -v
```

Expected: fail because `workflow_node_execution_activemq_repository.py` does not exist.

### Task 4: ActiveMQ Producer Repository Implementation

**Files:**
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/repositories/workflow_node_execution_activemq_repository.py`

- [ ] **Step 1: Add minimal producer repository**

```python
"""ActiveMQ publisher repository for workflow node execution snapshots.

This repository is producer-only. The API process keeps an in-memory read cache
for workflow runtime lookups, while the external consumer owns durable database
writes and truncation.
"""

import json
import logging
import socket
import uuid
from collections.abc import Callable, Sequence
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


def _frame(command: str, headers: dict[str, str], body: bytes = b"") -> bytes:
    header_lines = "\n".join(f"{key}:{value}" for key, value in headers.items())
    return f"{command}\n{header_lines}\n\n".encode() + body + b"\x00"


class WorkflowNodeExecutionActiveMQPublisher:
    """Minimal STOMP 1.2 publisher using the stdlib socket module."""

    def publish(self, event: dict[str, Any]) -> None:
        body = json.dumps(event, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        headers = {
            "destination": dify_config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
            "content-type": "application/json",
            "event_type": event["event_type"],
            "schema_version": str(event["schema_version"]),
            "JMSXGroupID": event["payload"]["workflow_run_id"] or event["payload"]["id"],
            "content-length": str(len(body)),
        }
        for attempt in range(dify_config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES + 1):
            try:
                self._send(body, headers)
                return
            except OSError:
                if attempt >= dify_config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES:
                    raise

    def _send(self, body: bytes, headers: dict[str, str]) -> None:
        timeout = dify_config.WORKFLOW_LOG_PUBLISH_TIMEOUT
        with socket.create_connection(
            (dify_config.WORKFLOW_LOG_ACTIVEMQ_HOST, dify_config.WORKFLOW_LOG_ACTIVEMQ_PORT),
            timeout=timeout,
        ) as sock:
            sock.settimeout(timeout)
            connect_headers = {
                "accept-version": "1.2",
                "host": dify_config.WORKFLOW_LOG_ACTIVEMQ_HOST,
            }
            if dify_config.WORKFLOW_LOG_ACTIVEMQ_USERNAME:
                connect_headers["login"] = dify_config.WORKFLOW_LOG_ACTIVEMQ_USERNAME
                connect_headers["passcode"] = dify_config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD
            sock.sendall(_frame("CONNECT", connect_headers))
            sock.recv(1024)
            sock.sendall(_frame("SEND", headers, body))
            sock.sendall(_frame("DISCONNECT", {}))


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
        self._publish_event = publisher or WorkflowNodeExecutionActiveMQPublisher().publish
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
```

- [ ] **Step 2: Run repository tests**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py -v
```

Expected: all selected tests pass.

- [ ] **Step 3: Commit**

```bash
git add api/core/repositories/workflow_node_execution_activemq_repository.py \
  api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py
git commit -m "feat: publish workflow node executions to activemq"
```

### Task 5: Factory Routing Tests

**Files:**
- Create: `/Users/yang/.codex/worktrees/5ef0/dify/api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py`

- [ ] **Step 1: Add factory tests**

```python
from sqlalchemy import create_engine

from core.repositories.factory import DifyCoreRepositoryFactory
from core.repositories.sqlalchemy_workflow_node_execution_repository import SQLAlchemyWorkflowNodeExecutionRepository
from core.repositories.workflow_node_execution_activemq_repository import ActiveMQWorkflowNodeExecutionRepository
from models import Account, Tenant
from models.enums import WorkflowRunTriggeredFrom
from models.workflow import WorkflowNodeExecutionTriggeredFrom


def _account() -> Account:
    account = Account(name="Test", email="test@example.com")
    account.id = "user-id"
    tenant = Tenant(name="Tenant")
    tenant.id = "tenant-id"
    account._current_tenant = tenant
    return account


def _engine():
    return create_engine("sqlite:///:memory:")


def test_factory_uses_sqlalchemy_when_async_disabled(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", False)

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.APP_RUN,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_uses_activemq_for_app_workflow_runs(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.APP_RUN,
    )

    assert isinstance(repo, ActiveMQWorkflowNodeExecutionRepository)


def test_factory_keeps_debugging_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.DEBUGGING,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_keeps_single_step_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.SINGLE_STEP,
        workflow_triggered_from=WorkflowRunTriggeredFrom.DEBUGGING,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_keeps_rag_pipeline_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.RAG_PIPELINE_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.RAG_PIPELINE_RUN,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_keeps_missing_workflow_trigger_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py -v
```

Expected: fail because factory does not accept `workflow_triggered_from`.

### Task 6: Factory Routing Implementation

**Files:**
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/repositories/factory.py`

- [ ] **Step 1: Add optional routing argument**

Change the signature:

```python
def create_workflow_node_execution_repository(
    cls,
    session_factory: Union[sessionmaker, Engine],
    user: Union[Account, EndUser],
    app_id: str,
    triggered_from: WorkflowNodeExecutionTriggeredFrom,
    workflow_triggered_from: WorkflowRunTriggeredFrom | None = None,
) -> WorkflowNodeExecutionRepository:
```

- [ ] **Step 2: Add conservative ActiveMQ routing before configured class import**

```python
if (
    dify_config.WORKFLOW_LOG_ASYNC_ENABLED
    and dify_config.WORKFLOW_LOG_QUEUE_PROVIDER == "activemq"
    and triggered_from == WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN
    and workflow_triggered_from == WorkflowRunTriggeredFrom.APP_RUN
):
    from core.repositories.workflow_node_execution_activemq_repository import (
        ActiveMQWorkflowNodeExecutionRepository,
    )

    return ActiveMQWorkflowNodeExecutionRepository(
        user=user,
        app_id=app_id,
        triggered_from=triggered_from,
    )
```

- [ ] **Step 3: Run factory tests**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py -v
```

Expected: all selected tests pass.

- [ ] **Step 4: Commit**

```bash
git add api/core/repositories/factory.py \
  api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py
git commit -m "feat: route app workflow node logs to activemq"
```

### Task 7: App Generator Call Sites

**Files:**
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/app/apps/workflow/app_generator.py`
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/app/apps/advanced_chat/app_generator.py`
- Modify: `/Users/yang/.codex/worktrees/5ef0/dify/api/core/app/apps/pipeline/pipeline_generator.py`

- [ ] **Step 1: Pass workflow trigger in workflow app normal run**

In `workflow/app_generator.py`, the existing code computes `workflow_triggered_from`. Pass it to the node repository:

```python
workflow_node_execution_repository = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
    session_factory=session_factory,
    user=user,
    app_id=application_generate_entity.app_config.app_id,
    triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
    workflow_triggered_from=workflow_triggered_from,
)
```

Leave single-step calls unchanged or pass `WorkflowRunTriggeredFrom.DEBUGGING`; both stay synchronous because `triggered_from` is `SINGLE_STEP`.

- [ ] **Step 2: Pass workflow trigger in advanced chat normal run**

In `advanced_chat/app_generator.py`, after it computes `workflow_triggered_from`, pass it:

```python
workflow_node_execution_repository = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
    session_factory=session_factory,
    user=user,
    app_id=application_generate_entity.app_config.app_id,
    triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
    workflow_triggered_from=workflow_triggered_from,
)
```

- [ ] **Step 3: Pass RAG trigger in pipeline normal run**

In `pipeline/pipeline_generator.py`, after it computes `workflow_triggered_from`, pass it:

```python
workflow_node_execution_repository = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
    session_factory=session_factory,
    user=user,
    app_id=application_generate_entity.app_config.app_id,
    triggered_from=WorkflowNodeExecutionTriggeredFrom.RAG_PIPELINE_RUN,
    workflow_triggered_from=workflow_triggered_from,
)
```

This documents the conservative route and keeps RAG synchronous.

- [ ] **Step 4: Run factory and repository focused tests**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py \
  api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py -v
```

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add api/core/app/apps/workflow/app_generator.py \
  api/core/app/apps/advanced_chat/app_generator.py \
  api/core/app/apps/pipeline/pipeline_generator.py
git commit -m "chore: pass workflow trigger source to node log repository"
```

### Task 8: Final Focused Verification

**Files:**
- Verify all files touched by Tasks 1-7.

- [ ] **Step 1: Run the Dify backport test set**

Run:

```bash
uv run --project api pytest -o addopts='' \
  api/tests/unit_tests/configs/test_workflow_log_config.py \
  api/tests/unit_tests/migrations/test_workflow_node_execution_state_version.py \
  api/tests/unit_tests/models/test_workflow_models.py::TestWorkflowNodeExecutionModel::test_node_execution_state_version_is_nullable \
  api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py \
  api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py -v
```

Expected: all selected tests pass.

- [ ] **Step 2: Inspect diff for non-scope**

Run:

```bash
git diff --stat 1.13.3..HEAD
git diff --name-only 1.13.3..HEAD
```

Expected: only Dify API files from this plan appear. No consumer files, docker middleware files, offload writer files, or package refactor files.

- [ ] **Step 3: Confirm config stays default-off**

Run:

```bash
rg -n "WORKFLOW_LOG_ASYNC_ENABLED|WORKFLOW_LOG_QUEUE_PROVIDER|WORKFLOW_LOG_ACTIVEMQ" api/configs api/.env.example
```

Expected: `WORKFLOW_LOG_ASYNC_ENABLED` default is `False` in code and `false` in `.env.example`.

- [ ] **Step 4: Final commit if any verification-only fixes were needed**

If verification required edits, commit them:

```bash
git add api
git commit -m "test: cover activemq workflow node log backport"
```

If no edits were needed, do not create an empty commit.

## Self-Review

- Spec coverage:
  - Migration: Task 2.
  - Config default disabled: Task 2.
  - Producer-only repository: Task 4.
  - ActiveMQ event payload with `state_version`: Task 4.
  - Per-id in-memory `state_version`: Task 4.
  - Lock around cache and version increment: Task 4.
  - Publish outside lock: Task 3 and Task 4.
  - No rollback after publish failure: Task 3 and Task 4.
  - Route only `WORKFLOW_RUN + APP_RUN`: Task 6 and Task 7.
  - Debugger, single-step, RAG fallback: Task 5 and Task 7.
  - No offload or docker restructuring: Scope and Task 8.
- Placeholder scan: no deferred placeholders.
- Type consistency:
  - 1.13.3 imports use `dify_graph.*`, not current HEAD `graphon.*`.
  - Repository method is `get_by_workflow_run`, matching the 1.13.3 protocol.
  - Factory keeps `workflow_triggered_from` optional, preserving old callers.
