# Phoenix Parent Trace Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Prevent Phoenix/Arize ops trace tasks from retrying forever when a child workflow references a parent workflow whose app-level Phoenix/Arize tracing is disabled.

**Architecture:** Keep the normal Redis parent-carrier path for traced parents. When a parent carrier is missing, inspect the parent workflow run's app tracing config; if the parent cannot publish Phoenix/Arize carrier data, fall back to a synthetic root span keyed by the parent workflow run id. Preserve retry behavior when the parent is traceable or the parent run cannot be resolved.

**Tech Stack:** Python, SQLAlchemy models, OpenTelemetry trace context propagation, pytest.

---

## File Map

- `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
  - Add helper functions for parent workflow tracing-config inspection.
  - Adjust `workflow_trace()` parent carrier resolution to fallback only for untraceable parents.

- `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
  - Add unit tests for non-traceable parent fallback.
  - Add unit tests for traceable parent retry preservation.

- `docs/superpowers/specs/2026-05-16-phoenix-parent-trace-fallback-design.md`
  - Design document for this change.

## Task 1: Phoenix Parent Carrier Fallback

**Files:**
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`

- [x] **Step 1: Write failing fallback test**

Add this test to `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py` near the existing nested workflow tests:

```python
def test_nested_workflow_trace_falls_back_when_parent_app_tracing_disabled(monkeypatch):
    instance, tracer = _make_trace_instance(monkeypatch)
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get",
        lambda key: None,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._parent_workflow_can_publish_span_context",
        lambda parent_workflow_run_id: False,
    )

    instance.workflow_trace(
        _make_workflow_trace_info(
            workflow_run_id="child-run-123456",
            metadata={
                "app_name": "Child Workflow",
                "parent_trace_context": {
                    "parent_workflow_run_id": "outer-run",
                    "parent_node_execution_id": "outer-run:tool-exec-id",
                },
            },
        )
    )

    assert [span.name for span in tracer.spans] == ["child-run-123456", "nested_Child_Workflow_child-ru"]
    assert tracer.spans[1].attributes[SpanAttributes.SESSION_ID] == "outer-run"
```

- [x] **Step 2: Write failing retry-preservation test**

Add this test beside the fallback test:

```python
def test_nested_workflow_trace_still_retries_when_parent_app_can_publish_context(monkeypatch):
    instance, _ = _make_trace_instance(monkeypatch)
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get",
        lambda key: None,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._parent_workflow_can_publish_span_context",
        lambda parent_workflow_run_id: True,
    )

    with pytest.raises(PendingTraceParentContextError):
        instance.workflow_trace(
            _make_workflow_trace_info(
                workflow_run_id="child-run-123456",
                metadata={
                    "app_name": "Child Workflow",
                    "parent_trace_context": {
                        "parent_workflow_run_id": "outer-run",
                        "parent_node_execution_id": "outer-run:tool-exec-id",
                    },
                },
            )
        )
```

- [x] **Step 3: Run tests to verify failure**

Run:

```bash
pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_nested_workflow_trace_falls_back_when_parent_app_tracing_disabled api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_nested_workflow_trace_still_retries_when_parent_app_can_publish_context -q
```

Expected: the first test fails because `_parent_workflow_can_publish_span_context` does not exist, or because `PendingTraceParentContextError` is still raised.

- [x] **Step 4: Add parent publishability helper**

In `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`, import `App` and `WorkflowRun`:

```python
from models.model import App, EndUser, MessageFile
from models.workflow import WorkflowNodeExecutionModel, WorkflowRun
```

Add helper functions near `_resolve_published_parent_span_context()`:

```python
def _app_uses_phoenix_provider(app_tracing_config: Mapping[str, Any] | None) -> bool:
    if not app_tracing_config or not app_tracing_config.get("enabled"):
        return False
    return app_tracing_config.get("tracing_provider") in {"arize", "phoenix"}


def _parent_workflow_can_publish_span_context(parent_workflow_run_id: str) -> bool:
    parent_run = db.session.query(WorkflowRun).filter(WorkflowRun.id == parent_workflow_run_id).first()
    if parent_run is None:
        return True

    parent_app = db.session.query(App).filter(App.id == parent_run.app_id).first()
    if parent_app is None or not parent_app.tracing:
        return False

    try:
        app_tracing_config = json.loads(parent_app.tracing)
    except (TypeError, json.JSONDecodeError):
        return False

    return _app_uses_phoenix_provider(app_tracing_config)
```

- [x] **Step 5: Add carrier resolution fallback helper**

Add this helper near the parent publishability helper:

```python
def _resolve_workflow_parent_carrier(
    parent_node_execution_id: str,
    parent_workflow_run_id: str | None,
) -> dict[str, str] | None:
    try:
        return _resolve_published_parent_span_context(parent_node_execution_id)
    except PendingTraceParentContextError:
        if parent_workflow_run_id and not _parent_workflow_can_publish_span_context(parent_workflow_run_id):
            logger.info(
                "[Arize/Phoenix] Parent workflow %s cannot publish parent span context; using fallback root",
                parent_workflow_run_id,
            )
            return None
        raise
```

- [x] **Step 6: Use fallback in `workflow_trace()`**

Replace:

```python
if parent_node_execution_id:
    carrier = _resolve_published_parent_span_context(parent_node_execution_id)
else:
    trace_id_source = parent_workflow_run_id or trace_info.workflow_run_id or trace_info.message_id
    carrier = self.ensure_root_span(
```

with:

```python
carrier = (
    _resolve_workflow_parent_carrier(parent_node_execution_id, parent_workflow_run_id)
    if parent_node_execution_id
    else None
)
if carrier is None:
    trace_id_source = parent_workflow_run_id or trace_info.workflow_run_id or trace_info.message_id
    carrier = self.ensure_root_span(
```

Keep the existing `ensure_root_span()` arguments unchanged.

- [x] **Step 7: Run focused tests**

Run:

```bash
pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_nested_workflow_trace_falls_back_when_parent_app_tracing_disabled api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_nested_workflow_trace_still_retries_when_parent_app_can_publish_context -q
```

Expected: both tests pass.

- [x] **Step 8: Run related regression tests**

Run:

```bash
pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py api/tests/unit_tests/tasks/test_ops_trace_task.py -q
```

Expected: all tests pass.

- [x] **Step 9: Self-review**

Check:

```bash
git diff -- api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py docs/superpowers/specs/2026-05-16-phoenix-parent-trace-fallback-design.md docs/superpowers/plans/2026-05-16-phoenix-parent-trace-fallback.md
```

Expected:
- Missing parent carrier still raises `PendingTraceParentContextError` when parent app can publish Phoenix/Arize context.
- Missing parent carrier falls back only when parent app cannot publish Phoenix/Arize context.
- Invalid carrier contents are still hard errors.
- No commits are created during task execution; final commit happens once after review.
