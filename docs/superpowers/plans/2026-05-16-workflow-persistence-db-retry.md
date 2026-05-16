# Workflow Persistence DB Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Prevent short PostgreSQL restarts or failovers from aborting workflow/chatflow streaming pipelines when workflow run or node execution persistence hits a transient DB error.

**Architecture:** Add one small SQLAlchemy retry helper used by both workflow persistence repositories. Keep retry scope narrow: log every SQLAlchemy DB exception at error level, retry only transient DB errors, create a fresh session per attempt, then re-raise after retry exhaustion or non-transient DB failures.

**Tech Stack:** Python 3.12, SQLAlchemy 2.x, pytest, pytest caplog, ruff.

---

## File Structure

- Create: `api/core/repositories/sqlalchemy_retry.py`
  - Shared retry helper for short SQLAlchemy persistence operations.
  - Owns transient error classification, exponential delay with subsecond jitter, error logging, rollback, and retry loop.
- Modify: `api/core/repositories/sqlalchemy_workflow_execution_repository.py`
  - Replace local workflow run retry loop with the shared helper.
  - Preserve existing semantics and cache update after successful commit.
- Modify: `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`
  - Wrap `session.merge(db_model)` and `session.commit()` in the same shared retry helper.
  - Preserve existing node execution cache update after successful commit.
- Modify: `api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py`
  - Update tests to patch the shared delay function instead of repository-local constants.
  - Add one assertion that workflow run retry uses a second session after transient DB failure.
- Modify: `api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py`
  - Add node execution retry/logging tests mirroring the workflow run tests.
  - Keep existing node repository tests intact.

## Design Rules

- Retry only exceptions derived from `SQLAlchemyError`.
- Error-log every caught `SQLAlchemyError` with `exc_info=True`.
- Retry only `OperationalError` and `DBAPIError` with `connection_invalidated=True`.
- Do not retry non-DB exceptions such as `ValueError`, JSON encoding failures, or invalid repository configuration.
- Attempts: 4 total attempts.
- Delay: exponential backoff with subsecond jitter: attempt 1 retry waits `2s + jitter`, attempt 2 retry waits `4s + jitter`, attempt 3 retry waits `8s + jitter`; jitter is `random.uniform(0, 1)`.
- Use a new session for each attempt by calling `session_factory()` inside the retry loop.
- Try `session.rollback()` after a caught DB exception before leaving the failed session context.
- After final failure, re-raise the original SQLAlchemy exception.
- Do not add durable queue, reconciliation job, or final-state inference in this change.

---

### Task 1: Add Failing Node Execution Retry Tests

**Files:**
- Modify: `api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py`

- [x] **Step 1: Add SQLAlchemy exception imports**

Add these imports near the existing SQLAlchemy imports:

```python
from sqlalchemy.exc import OperationalError, SQLAlchemyError
```

- [x] **Step 2: Add a reusable mock session helper**

Add this helper below the existing `session` fixture:

```python
def _session():
    session = MagicMock(spec=Session)
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=None)
    return session
```

- [x] **Step 3: Add failing retry test for transient node save failure**

Add this test below `test_save_with_existing_tenant_id`:

```python
def test_save_retries_transient_commit_error(repository, monkeypatch):
    first_session = _session()
    second_session = _session()
    first_session.commit.side_effect = OperationalError(
        "UPDATE workflow_node_executions",
        {},
        RuntimeError("db restarting"),
    )

    db_model = MagicMock(spec=WorkflowNodeExecutionModel)
    db_model.node_execution_id = "test-node-execution-id"
    repository.to_db_model = MagicMock(return_value=db_model)
    repository._session_factory.side_effect = [first_session, second_session]
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", MagicMock())

    repository.save(MagicMock(spec=WorkflowNodeExecution))

    assert repository._session_factory.call_count == 2
    first_session.merge.assert_called_once_with(db_model)
    first_session.commit.assert_called_once()
    second_session.merge.assert_called_once_with(db_model)
    second_session.commit.assert_called_once()
    assert repository._node_execution_cache["test-node-execution-id"] is db_model
```

- [x] **Step 4: Add failing logging test for non-retryable DB exception**

Add this test after the transient retry test:

```python
def test_save_logs_error_for_db_exception_without_retry(repository, caplog):
    session = _session()
    session.commit.side_effect = SQLAlchemyError("db constraint failed")

    db_model = MagicMock(spec=WorkflowNodeExecutionModel)
    db_model.node_execution_id = "test-node-execution-id"
    repository.to_db_model = MagicMock(return_value=db_model)
    repository._session_factory.return_value = session

    with pytest.raises(SQLAlchemyError, match="db constraint failed"):
        repository.save(MagicMock(spec=WorkflowNodeExecution))

    repository._session_factory.assert_called_once()
    session.commit.assert_called_once()
    assert "Workflow node execution persistence error" in caplog.text
    assert "test-node-execution-id" in caplog.text
```

- [x] **Step 5: Verify tests fail for the expected reason**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py::test_save_retries_transient_commit_error tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py::test_save_logs_error_for_db_exception_without_retry -q
```

Expected: both tests fail. The retry test should fail because `SQLAlchemyWorkflowNodeExecutionRepository.save()` currently raises the first `OperationalError`. The log test should fail because no node persistence error log is emitted.

---

### Task 2: Add Shared SQLAlchemy Retry Helper

**Files:**
- Create: `api/core/repositories/sqlalchemy_retry.py`

- [x] **Step 1: Create helper module**

Create `api/core/repositories/sqlalchemy_retry.py` with:

```python
import logging
import random
import time
from collections.abc import Callable
from typing import TypeVar

from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

T = TypeVar("T")

SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS = 4
SQLALCHEMY_PERSISTENCE_RETRY_BASE_DELAY_SECONDS = 2
SQLALCHEMY_PERSISTENCE_RETRY_MAX_DELAY_SECONDS = 8
SQLALCHEMY_PERSISTENCE_RETRY_JITTER_SECONDS = 1


def execute_with_db_retry(
    *,
    operation: Callable[[], T],
    rollback: Callable[[], None],
    logger: logging.Logger,
    operation_name: str,
    context: str,
) -> T:
    for attempt in range(1, SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS + 1):
        try:
            return operation()
        except SQLAlchemyError as exc:
            logger.error(
                "%s persistence error for %s (attempt %s/%s)",
                operation_name,
                context,
                attempt,
                SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
                exc_info=True,
            )
            if not is_retryable_db_error(exc) or attempt >= SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS:
                raise

            try:
                rollback()
            except Exception:
                logger.debug("Failed to rollback %s persistence retry session", operation_name, exc_info=True)

            next_attempt = attempt + 1
            logger.info(
                "Retrying %s persistence for %s (attempt %s/%s)",
                operation_name,
                context,
                next_attempt,
                SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
            )
            _sleep_before_retry(attempt)

    raise RuntimeError("unreachable workflow persistence retry state")


def is_retryable_db_error(exc: SQLAlchemyError) -> bool:
    return isinstance(exc, OperationalError) or (
        isinstance(exc, DBAPIError) and bool(getattr(exc, "connection_invalidated", False))
    )


def _sleep_before_retry(attempt: int) -> None:
    delay = min(
        SQLALCHEMY_PERSISTENCE_RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)),
        SQLALCHEMY_PERSISTENCE_RETRY_MAX_DELAY_SECONDS,
    )
    delay += random.uniform(0, SQLALCHEMY_PERSISTENCE_RETRY_JITTER_SECONDS)
    time.sleep(delay)
```

- [x] **Step 2: Run ruff on helper**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run ruff check core/repositories/sqlalchemy_retry.py
```

Expected: pass.

---

### Task 3: Use Helper in Node Execution Repository

**Files:**
- Modify: `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`

- [x] **Step 1: Import helper**

Add this import near repository imports:

```python
from core.repositories.sqlalchemy_retry import execute_with_db_retry
```

- [x] **Step 2: Replace direct node save transaction**

Replace the body of `save()` after `db_model = self.to_db_model(execution)` with:

```python
        with self._session_factory() as session:

            def operation():
                session.merge(db_model)
                session.commit()

            execute_with_db_retry(
                operation=operation,
                rollback=session.rollback,
                logger=logger,
                operation_name="Workflow node execution",
                context=f"node_execution_id {db_model.node_execution_id or db_model.id}",
            )

            # Update the in-memory cache for faster subsequent lookups
            # Only cache if we have a node_execution_id to use as the cache key
            if db_model.node_execution_id:
                logger.debug(f"Updating cache for node_execution_id: {db_model.node_execution_id}")
                self._node_execution_cache[db_model.node_execution_id] = db_model
```

- [x] **Step 3: Run node repository tests**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -q
```

Expected: fail if the implementation uses the same session for every retry. If that happens, continue to Task 4 and adjust helper usage to create a fresh session per attempt.

---

### Task 4: Refine Helper to Own Fresh Session Per Attempt

**Files:**
- Modify: `api/core/repositories/sqlalchemy_retry.py`
- Modify: `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`

- [x] **Step 1: Change helper signature to receive a session factory**

Replace `execute_with_db_retry()` in `sqlalchemy_retry.py` with:

```python
def execute_with_db_retry(
    *,
    session_factory: Callable[[], object],
    operation: Callable[[object], T],
    logger: logging.Logger,
    operation_name: str,
    context: str,
) -> T:
    for attempt in range(1, SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS + 1):
        with session_factory() as session:
            try:
                return operation(session)
            except SQLAlchemyError as exc:
                logger.error(
                    "%s persistence error for %s (attempt %s/%s)",
                    operation_name,
                    context,
                    attempt,
                    SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
                    exc_info=True,
                )
                if not is_retryable_db_error(exc) or attempt >= SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS:
                    raise

                try:
                    session.rollback()
                except Exception:
                    logger.debug("Failed to rollback %s persistence retry session", operation_name, exc_info=True)

                next_attempt = attempt + 1
                logger.info(
                    "Retrying %s persistence for %s (attempt %s/%s)",
                    operation_name,
                    context,
                    next_attempt,
                    SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
                )

        _sleep_before_retry(attempt)

    raise RuntimeError("unreachable workflow persistence retry state")
```

- [x] **Step 2: Update node repository save to pass session factory**

Replace the `with self._session_factory() as session:` retry block with:

```python
        def operation(session):
            session.merge(db_model)
            session.commit()

        execute_with_db_retry(
            session_factory=self._session_factory,
            operation=operation,
            logger=logger,
            operation_name="Workflow node execution",
            context=f"node_execution_id {db_model.node_execution_id or db_model.id}",
        )

        # Update the in-memory cache for faster subsequent lookups
        # Only cache if we have a node_execution_id to use as the cache key
        if db_model.node_execution_id:
            logger.debug(f"Updating cache for node_execution_id: {db_model.node_execution_id}")
            self._node_execution_cache[db_model.node_execution_id] = db_model
```

- [x] **Step 3: Run node repository tests**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run pytest tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py -q
```

Expected: pass.

---

### Task 5: Move Workflow Run Repository to Shared Helper

**Files:**
- Modify: `api/core/repositories/sqlalchemy_workflow_execution_repository.py`
- Modify: `api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py`

- [x] **Step 1: Replace local retry imports**

In `sqlalchemy_workflow_execution_repository.py`, remove:

```python
import time
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError
```

Add:

```python
from core.repositories.sqlalchemy_retry import execute_with_db_retry
```

- [x] **Step 2: Remove local retry constants and classifier**

Delete the prior local workflow execution retry constants.

Delete the `_is_retryable_save_error()` static method.

- [x] **Step 3: Replace local retry loop**

Replace the loop inside `save()` after `db_model = self._to_db_model(execution)` with:

```python
        def operation(session):
            session.merge(db_model)
            session.commit()

        execute_with_db_retry(
            session_factory=self._session_factory,
            operation=operation,
            logger=logger,
            operation_name="Workflow execution",
            context=f"execution_id {db_model.id}",
        )

        # Update the in-memory cache for faster subsequent lookups
        logger.debug(f"Updating cache for execution_id: {db_model.id}")
        self._execution_cache[db_model.id] = db_model
```

- [x] **Step 4: Update workflow execution test delay patch**

In `test_save_retries_transient_commit_error`, replace:

```python
    monkeypatch.setattr(
        "core.repositories.sqlalchemy_workflow_execution_repository._WORKFLOW_EXECUTION_SAVE_RETRY_DELAY_SECONDS", 0
    )
```

with:

```python
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", MagicMock())
```

- [x] **Step 5: Run workflow execution repository tests**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run pytest tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py -q
```

Expected: pass.

---

### Task 6: Add Helper Unit Tests for Backoff and Error Classification

**Files:**
- Create: `api/tests/unit_tests/repositories/test_sqlalchemy_retry.py`

- [x] **Step 1: Add helper tests**

Create `api/tests/unit_tests/repositories/test_sqlalchemy_retry.py`:

```python
from unittest.mock import MagicMock

from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

from core.repositories.sqlalchemy_retry import execute_with_db_retry, is_retryable_db_error


def _session():
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=None)
    return session


def test_is_retryable_db_error_accepts_operational_error():
    exc = OperationalError("SELECT 1", {}, RuntimeError("db restarting"))

    assert is_retryable_db_error(exc)


def test_is_retryable_db_error_accepts_invalidated_dbapi_error():
    exc = DBAPIError("SELECT 1", {}, RuntimeError("connection invalidated"), connection_invalidated=True)

    assert is_retryable_db_error(exc)


def test_is_retryable_db_error_rejects_generic_sqlalchemy_error():
    assert not is_retryable_db_error(SQLAlchemyError("constraint failed"))


def test_execute_with_db_retry_retries_with_new_session(monkeypatch):
    first_session = _session()
    second_session = _session()
    session_factory = MagicMock(side_effect=[first_session, second_session])
    logger = MagicMock()
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", MagicMock())

    operation = MagicMock(
        side_effect=[
            OperationalError("UPDATE workflow_runs", {}, RuntimeError("db restarting")),
            "ok",
        ]
    )

    result = execute_with_db_retry(
        session_factory=session_factory,
        operation=operation,
        logger=logger,
        operation_name="Workflow execution",
        context="execution_id test",
    )

    assert result == "ok"
    assert session_factory.call_count == 2
    operation.assert_any_call(first_session)
    operation.assert_any_call(second_session)
    first_session.rollback.assert_called_once()
    logger.error.assert_called_once()
    logger.info.assert_called_once()
```

- [x] **Step 2: Run helper tests**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run pytest tests/unit_tests/repositories/test_sqlalchemy_retry.py -q
```

Expected: pass.

---

### Task 7: Full Verification and Commit

**Files:**
- Verify all modified files.

- [x] **Step 1: Run focused tests**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run pytest \
  tests/unit_tests/repositories/test_sqlalchemy_retry.py \
  tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/core/workflow/test_workflow_cycle_manager.py \
  -q
```

Expected: all tests pass. Existing coverage warnings about `./api` may still appear; do not treat those as failures when exit code is 0.

- [x] **Step 2: Run ruff**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify/api
uv run ruff check \
  core/repositories/sqlalchemy_retry.py \
  core/repositories/sqlalchemy_workflow_execution_repository.py \
  core/repositories/sqlalchemy_workflow_node_execution_repository.py \
  tests/unit_tests/repositories/test_sqlalchemy_retry.py \
  tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py \
  tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py
```

Expected: `All checks passed!`

- [x] **Step 3: Inspect diff**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify
git diff --stat
git diff -- api/core/repositories/sqlalchemy_retry.py \
  api/core/repositories/sqlalchemy_workflow_execution_repository.py \
  api/core/repositories/sqlalchemy_workflow_node_execution_repository.py \
  api/tests/unit_tests/repositories/test_sqlalchemy_retry.py \
  api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py \
  api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py
```

Expected: diff only contains retry helper, repository retry integration, and focused tests.

- [x] **Step 4: Commit**

Run:

```bash
cd /Users/yang/.codex/worktrees/54c7/dify
git add \
  api/core/repositories/sqlalchemy_retry.py \
  api/core/repositories/sqlalchemy_workflow_execution_repository.py \
  api/core/repositories/sqlalchemy_workflow_node_execution_repository.py \
  api/tests/unit_tests/repositories/test_sqlalchemy_retry.py \
  api/tests/unit_tests/repositories/workflow_execution/test_sqlalchemy_repository.py \
  api/tests/unit_tests/repositories/workflow_node_execution/test_sqlalchemy_repository.py
git commit -m "fix: retry workflow persistence db errors"
```

Expected: commit succeeds.

---

## Manual Validation

After automated tests pass, validate the observed failure mode locally:

1. Start Dify API and dependencies.
2. Start a workflow draft run in streaming mode.
3. Restart PostgreSQL during a node execution.
4. Bring PostgreSQL back within the retry window.
5. Confirm logs contain an error entry for workflow node execution persistence and retry info.
6. Confirm the stream pipeline continues if DB recovers inside the retry window.
7. Confirm `workflow_runs.status` eventually changes from `running` to terminal status.

If PostgreSQL stays down longer than the retry window, the request may still fail. That behavior is expected for this plan.

## Self-Review

- Spec coverage: The plan covers the observed node execution failure and the existing workflow run status update retry.
- Placeholder scan: No placeholders or deferred steps remain.
- Type consistency: Helper signatures use `session_factory` and `operation(session)` consistently across tasks.
- Scope check: Durable recovery for long DB outages is explicitly excluded.
