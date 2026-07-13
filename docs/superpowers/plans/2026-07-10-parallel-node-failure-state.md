# Parallel Node Failure State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve the real failed node result while marking unfinished parallel nodes as stopped with structured failure-cause outputs.

**Architecture:** Record the originating node ID in `GraphExecution`, expose it on `GraphRunFailedEvent`, and let the persistence layer distinguish the failed node from collateral in-flight executions. Reuse the existing `stopped` status and API fields; no frontend changes or new status values are needed.

**Tech Stack:** Python 3.12, Pydantic v2, pytest, Ruff

---

## File Structure

- Modify `api/dify_graph/graph_engine/domain/graph_execution.py`: retain the originating failed node ID in runtime and serialized graph state.
- Modify `api/dify_graph/graph_engine/event_management/event_handlers.py`: identify the node that caused fail-fast termination.
- Modify `api/dify_graph/graph_events/graph.py`: add the optional originating node ID to the graph failure event.
- Modify `api/dify_graph/graph_engine/graph_engine.py`: copy the recorded node ID into `GraphRunFailedEvent`.
- Modify `api/core/app/workflow/layers/persistence.py`: stop collateral running/retry executions and persist structured cause outputs.
- Modify `api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py`: cover failure-source state serialization.
- Modify `api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py`: cover failure-source capture.
- Modify `api/tests/unit_tests/core/app/workflow/test_persistence_layer.py`: cover failed, stopped, retry, and already-completed node persistence.

### Task 1: Carry the Originating Node ID to the Graph Failure Event

**Files:**
- Modify: `api/dify_graph/graph_engine/domain/graph_execution.py:31-49, 102-135, 180-230`
- Modify: `api/dify_graph/graph_engine/event_management/event_handlers.py:225-249`
- Modify: `api/dify_graph/graph_events/graph.py:25-28`
- Modify: `api/dify_graph/graph_engine/graph_engine.py:313-319`
- Test: `api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py`
- Test: `api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py`

- [ ] **Step 1: Write the failing GraphExecution serialization test**

Update the existing round-trip test to identify the failure source and assert it survives serialization:

```python
execution.fail(CustomGraphExecutionError("serialization failure"), failed_node_id="node-a")

assert payload["failed_node_id"] == "node-a"
assert restored.failed_node_id == "node-a"
```

- [ ] **Step 2: Write the failing event-handler test**

Give `_StubErrorHandler` the abort behavior used by a node without retry or an error strategy:

```python
class _StubErrorHandler:
    """Minimal error handler stub for tests."""

    def handle_node_failure(self, _event: NodeRunFailedEvent) -> None:
        return None
```

Import `NodeRunFailedEvent` and add:

```python
def test_failed_node_is_recorded_as_graph_failure_source() -> None:
    node_id = "test-node"
    handler, _, graph_execution = _build_event_handler(node_id)
    graph_execution.start()

    handler.dispatch(
        NodeRunFailedEvent(
            id="exec-1",
            node_id=node_id,
            node_type=BuiltinNodeTypes.CODE,
            node_title="Stub Node",
            start_at=naive_utc_now(),
            finished_at=naive_utc_now(),
            error="boom",
            node_run_result=NodeRunResult(
                status=WorkflowNodeExecutionStatus.FAILED,
                error="boom",
            ),
        )
    )

    assert graph_execution.failed_node_id == node_id
    assert graph_execution.error_message == "boom"
```

- [ ] **Step 3: Run the focused tests to verify they fail**

Run:

```bash
uv run --project api pytest -q \
  api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py::test_graph_execution_serialization_round_trip \
  api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py::test_failed_node_is_recorded_as_graph_failure_source
```

Expected: FAIL because `GraphExecution.fail()` does not accept or retain `failed_node_id`.

- [ ] **Step 4: Add the failure-source field to GraphExecution**

Add this optional field after `error` in `GraphExecutionState`:

```python
failed_node_id: str | None = Field(default=None, description="Node that caused graph failure")
```

Add this field after `error` in the `GraphExecution` dataclass:

```python
failed_node_id: str | None = None
```

Update `fail`, `dumps`, and `loads`:

```python
def fail(self, error: Exception, *, failed_node_id: str | None = None) -> None:
    """Mark the graph execution as failed and retain the originating node when known."""
    self.error = error
    self.failed_node_id = failed_node_id
    self.completed = True
```

```python
state = GraphExecutionState(
    workflow_id=self.workflow_id,
    started=self.started,
    completed=self.completed,
    aborted=self.aborted,
    paused=self.paused,
    pause_reasons=self.pause_reasons,
    error=_serialize_error(self.error),
    failed_node_id=self.failed_node_id,
    exceptions_count=self.exceptions_count,
    node_executions=node_states,
)
```

Add the failure-source assignment immediately after restoring `error` in `loads()`:

```python
self.failed_node_id = state.failed_node_id
```

The new serialized field stays optional, so existing version `1.0` snapshots remain valid.

- [ ] **Step 5: Capture the source in the fatal node-failure path**

Change only the no-retry/no-error-strategy branch in `EventHandler`:

```python
self._graph_execution.fail(RuntimeError(event.error), failed_node_id=event.node_id)
```

Leave `ExecutionCoordinator.mark_failed(error)` unchanged; dispatcher/internal failures have no originating node.

- [ ] **Step 6: Expose the source on GraphRunFailedEvent**

Add the backwards-compatible event field:

```python
class GraphRunFailedEvent(BaseGraphEvent):
    error: str = Field(..., description="failed reason")
    exceptions_count: int = Field(description="exception count", default=0)
    failed_node_id: str | None = Field(default=None, description="node that caused the graph failure")
```

Populate it in `GraphEngine.run()`:

```python
failed_event = GraphRunFailedEvent(
    error=str(e),
    exceptions_count=self._graph_execution.exceptions_count,
    failed_node_id=self._graph_execution.failed_node_id,
)
```

- [ ] **Step 7: Run the focused tests to verify they pass**

Run:

```bash
uv run --project api pytest -q \
  api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py::test_graph_execution_serialization_round_trip \
  api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py::test_failed_node_is_recorded_as_graph_failure_source
```

Expected: `2 passed`.

- [ ] **Step 8: Commit the failure-source propagation**

```bash
git add \
  api/dify_graph/graph_engine/domain/graph_execution.py \
  api/dify_graph/graph_engine/event_management/event_handlers.py \
  api/dify_graph/graph_events/graph.py \
  api/dify_graph/graph_engine/graph_engine.py \
  api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py \
  api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py
git commit -m "fix: retain workflow failure source node"
```

### Task 2: Stop Collateral In-Flight Nodes With Cause Outputs

**Files:**
- Modify: `api/core/app/workflow/layers/persistence.py:17-18, 196-205, 417-425`
- Test: `api/tests/unit_tests/core/app/workflow/test_persistence_layer.py:137-155, 194-216`

- [ ] **Step 1: Write the failing persistence test**

Import `TypedDict` in the implementation later, but keep the test data as ordinary mappings. Add this behavior test:

```python
def test_graph_failure_stops_other_in_flight_nodes_with_cause_outputs(self):
    layer, _, node_repo, _ = _make_layer()
    layer._handle_graph_run_started()
    created_at = _naive_utc_now()

    failed = WorkflowNodeExecution(
        id="failed-exec",
        workflow_id="workflow-id",
        workflow_execution_id="run-id",
        index=1,
        node_id="failed-node",
        node_type=BuiltinNodeTypes.HTTP_REQUEST,
        title="KB Retrieve",
        status=WorkflowNodeExecutionStatus.FAILED,
        inputs={"query": "hello"},
        outputs={"status_code": 400},
        error="Request failed with status code 400",
        created_at=created_at,
    )
    running = WorkflowNodeExecution(
        id="running-exec",
        workflow_id="workflow-id",
        workflow_execution_id="run-id",
        index=2,
        node_id="running-node",
        node_type=BuiltinNodeTypes.TOOL,
        title="Search",
        created_at=created_at,
    )
    retrying = WorkflowNodeExecution(
        id="retry-exec",
        workflow_id="workflow-id",
        workflow_execution_id="run-id",
        index=3,
        node_id="retry-node",
        node_type=BuiltinNodeTypes.HTTP_REQUEST,
        title="Memory",
        status=WorkflowNodeExecutionStatus.RETRY,
        created_at=created_at,
    )
    succeeded = WorkflowNodeExecution(
        id="succeeded-exec",
        workflow_id="workflow-id",
        workflow_execution_id="run-id",
        index=4,
        node_id="succeeded-node",
        node_type=BuiltinNodeTypes.CODE,
        title="Preprocess",
        status=WorkflowNodeExecutionStatus.SUCCEEDED,
        outputs={"result": "kept"},
        created_at=created_at,
    )
    for execution in (failed, running, retrying, succeeded):
        layer._node_execution_cache[execution.id] = execution

    layer._handle_graph_run_failed(
        GraphRunFailedEvent(
            error="Request failed with status code 400",
            exceptions_count=1,
            failed_node_id="failed-node",
        )
    )

    expected_outputs = {
        "failed_node_id": "failed-node",
        "failed_node_title": "KB Retrieve",
        "error": "Request failed with status code 400",
    }
    assert failed.status == WorkflowNodeExecutionStatus.FAILED
    assert failed.inputs == {"query": "hello"}
    assert failed.outputs == {"status_code": 400}
    assert failed.error == "Request failed with status code 400"
    assert running.status == WorkflowNodeExecutionStatus.STOPPED
    assert running.outputs == expected_outputs
    assert running.error is None
    assert retrying.status == WorkflowNodeExecutionStatus.STOPPED
    assert retrying.outputs == expected_outputs
    assert retrying.error is None
    assert succeeded.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert succeeded.outputs == {"result": "kept"}
    assert running in node_repo.saved_exec_data
    assert retrying in node_repo.saved_exec_data
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
uv run --project api pytest -q \
  api/tests/unit_tests/core/app/workflow/test_persistence_layer.py::TestWorkflowPersistenceLayer::test_graph_failure_stops_other_in_flight_nodes_with_cause_outputs
```

Expected: FAIL because collateral executions are currently marked `failed`, retrying executions are skipped, and no outputs are written.

- [ ] **Step 3: Add the typed cause payload and stopping helper**

Import `TypedDict` and define the internal payload near the persistence dataclasses:

```python
from typing import Any, TypedDict, Union


class _StoppedNodeOutputs(TypedDict):
    failed_node_id: str
    failed_node_title: str
    error: str
```

Add a focused helper without changing abort behavior:

```python
def _stop_in_flight_node_executions(
    self,
    *,
    failed_node_id: str,
    failed_node_title: str,
    error: str,
) -> None:
    now = naive_utc_now()
    outputs: _StoppedNodeOutputs = {
        "failed_node_id": failed_node_id,
        "failed_node_title": failed_node_title,
        "error": error,
    }
    for execution in self._node_execution_cache.values():
        if execution.status not in {
            WorkflowNodeExecutionStatus.RUNNING,
            WorkflowNodeExecutionStatus.RETRY,
        }:
            continue
        execution.status = WorkflowNodeExecutionStatus.STOPPED
        execution.outputs = outputs
        execution.finished_at = now
        execution.elapsed_time = max((now - execution.created_at).total_seconds(), 0.0)
        self._workflow_node_execution_repository.save(execution)
        self._workflow_node_execution_repository.save_execution_data(execution)
```

Do not set `execution.error`: the error belongs to the originating failed node and workflow run, while the stopped node exposes the cause through the agreed outputs.

- [ ] **Step 4: Route node-originated graph failures through the stopping helper**

Update `_handle_graph_run_failed` while retaining the existing fallback for non-node engine failures:

```python
if event.failed_node_id:
    failed_node_title = next(
        (
            node_execution.title
            for node_execution in self._node_execution_cache.values()
            if node_execution.node_id == event.failed_node_id
        ),
        event.failed_node_id,
    )
    self._stop_in_flight_node_executions(
        failed_node_id=event.failed_node_id,
        failed_node_title=failed_node_title,
        error=event.error,
    )
else:
    self._fail_running_node_executions(error_message=event.error)
```

Leave `_handle_graph_run_aborted` on its existing path; abort semantics are explicitly outside this change.

- [ ] **Step 5: Run persistence tests to verify they pass**

Run:

```bash
uv run --project api pytest -q api/tests/unit_tests/core/app/workflow/test_persistence_layer.py
```

Expected: all tests pass.

- [ ] **Step 6: Commit the persistence behavior**

```bash
git add \
  api/core/app/workflow/layers/persistence.py \
  api/tests/unit_tests/core/app/workflow/test_persistence_layer.py
git commit -m "fix: stop collateral nodes after workflow failure"
```

### Task 3: Verify Compatibility and Scope

**Files:**
- Verify only; no new files.

- [ ] **Step 1: Run formatting and lint checks on changed Python files**

```bash
uv run --project api ruff format --check \
  api/dify_graph/graph_engine/domain/graph_execution.py \
  api/dify_graph/graph_engine/event_management/event_handlers.py \
  api/dify_graph/graph_events/graph.py \
  api/dify_graph/graph_engine/graph_engine.py \
  api/core/app/workflow/layers/persistence.py \
  api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py \
  api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py \
  api/tests/unit_tests/core/app/workflow/test_persistence_layer.py

uv run --project api ruff check \
  api/dify_graph/graph_engine/domain/graph_execution.py \
  api/dify_graph/graph_engine/event_management/event_handlers.py \
  api/dify_graph/graph_events/graph.py \
  api/dify_graph/graph_engine/graph_engine.py \
  api/core/app/workflow/layers/persistence.py \
  api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py \
  api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py \
  api/tests/unit_tests/core/app/workflow/test_persistence_layer.py
```

Expected: both commands exit successfully with no findings.

- [ ] **Step 2: Run the complete affected unit-test groups**

```bash
uv run --project api pytest -q \
  api/tests/unit_tests/core/workflow/graph_engine/test_graph_execution_serialization.py \
  api/tests/unit_tests/core/workflow/graph_engine/event_management/test_event_handlers.py \
  api/tests/unit_tests/core/app/workflow/test_persistence_layer.py
```

Expected: all tests pass.

- [ ] **Step 3: Confirm the frontend requires no change**

```bash
rg -n "status === 'stopped'" \
  web/app/components/workflow/nodes/_base/components/node-status-icon.tsx
```

Expected: the existing stopped-status rendering condition is found.

- [ ] **Step 4: Confirm only intended files changed**

```bash
git diff --stat HEAD~2..HEAD
git status --short
```

Expected: the two implementation commits contain only the eight listed Python files; pre-existing unrelated working-tree changes remain unstaged.
