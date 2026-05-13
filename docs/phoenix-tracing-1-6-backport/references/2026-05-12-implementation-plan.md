# Phoenix Tracing 1.6 Backport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a temporary Dify 1.6-native backport of upstream Phoenix workflow tracing improvements, then keep it easy to revert before upgrading to 1.14.1 or newer.

**Architecture:** Keep the implementation in the 1.6 tracing and workflow execution paths. Add a small core parent-trace-context helper, pass private parent context from tool node to workflow-as-tool invocation, persist it into workflow trace metadata, then let the existing Phoenix provider reconstruct root/session/node hierarchy and coordinate nested workflow parent spans through Redis.

**Tech Stack:** Python, Flask, Celery, SQLAlchemy, Pydantic v2, OpenTelemetry, Redis, pytest, Dify 1.6 backend.

---

## Modification Scope

### Create

- `api/core/ops/exceptions.py`
  - Provider-neutral retry exceptions for ops trace dispatch.
- `api/core/ops/trace_context.py`
  - `ParentTraceContext` model and helpers for private parent trace metadata.
- `api/tests/unit_tests/core/ops/test_trace_context.py`
  - Unit tests for parent context validation and metadata resolution.
- `api/tests/unit_tests/tasks/test_ops_trace_task.py`
  - Unit tests for retryable trace task behavior.
- `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
  - Unit tests for Phoenix root/session/parent bridge helpers.

### Modify

- `api/configs/feature/__init__.py`
  - Add retry tuning config fields.
- `api/.env.example`
  - Document retry tuning env vars.
- `docker/middleware.env.example`
  - Document retry tuning env vars beside middleware/local Phoenix values.
- `api/core/ops/entities/trace_entity.py`
  - Add trace id resolution and parent context resolution on base trace info.
- `api/core/ops/ops_trace_manager.py`
  - Carry parent trace context from `TraceTask` into `WorkflowTraceInfo.metadata`.
- `api/core/app/apps/workflow/app_generator.py`
  - Extract parent context from private generator args into `WorkflowAppGenerateEntity.extras`.
- `api/core/app/apps/workflow/generate_task_pipeline.py`
  - Pass parent context from workflow generate entity extras into workflow trace task creation.
- `api/core/app/apps/advanced_chat/generate_task_pipeline.py`
  - Pass parent context for chatflow workflow completion paths.
- `api/core/workflow/workflow_cycle_manager.py`
  - Accept `parent_trace_context` for success, partial success, and failure trace tasks.
- `api/core/workflow/nodes/tool/tool_node.py`
  - Attach private parent context to workflow tool runtime before invocation.
- `api/core/tools/workflow_as_tool/tool.py`
  - Store private parent context and forward it to nested workflow generation without exposing it as user input.
- `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
  - Implement Phoenix canonical root/session behavior, node parenting, parent span Redis bridge, and pending-parent retry signal.
- Existing focused tests:
  - `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`
  - `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`

### Do Not Modify

- Database migrations.
- Frontend files.
- New Graphon or provider-package structure from later Dify versions.
- The already committed local-dev Phoenix middleware support, except the retry env examples named in this plan.

---

## Task 1: Parent Trace Context Core Helper

**Files:**
- Create: `api/core/ops/trace_context.py`
- Modify: `api/core/ops/entities/trace_entity.py`
- Test: `api/tests/unit_tests/core/ops/test_trace_context.py`

- [ ] **Step 1: Write failing tests for validation and metadata resolution**

Add this test file:

```python
from core.ops.entities.trace_entity import BaseTraceInfo
from core.ops.trace_context import (
    ParentTraceContext,
    extract_parent_trace_context_from_args,
    parent_trace_context_from_metadata,
)


def test_extract_parent_trace_context_from_args_accepts_complete_mapping():
    result = extract_parent_trace_context_from_args(
        {
            "parent_trace_context": {
                "parent_workflow_run_id": "outer-run",
                "parent_node_execution_id": "outer-run:tool-node",
            },
            "inputs": {"parent_trace_context": "user-input-must-stay-input"},
        }
    )

    assert result == {
        "parent_trace_context": ParentTraceContext(
            parent_workflow_run_id="outer-run",
            parent_node_execution_id="outer-run:tool-node",
        )
    }


def test_extract_parent_trace_context_from_args_rejects_incomplete_mapping():
    assert extract_parent_trace_context_from_args(
        {"parent_trace_context": {"parent_workflow_run_id": "outer-run"}}
    ) == {}


def test_parent_trace_context_from_metadata_accepts_model_and_mapping():
    model_context = ParentTraceContext(
        parent_workflow_run_id="outer-run",
        parent_node_execution_id="outer-run:tool-node",
    )

    assert parent_trace_context_from_metadata({"parent_trace_context": model_context}) == model_context
    assert parent_trace_context_from_metadata(
        {
            "parent_trace_context": {
                "parent_workflow_run_id": "outer-run",
                "parent_node_execution_id": "outer-run:tool-node",
            }
        }
    ) == model_context


def test_base_trace_info_resolved_parent_context_uses_private_metadata():
    trace_info = BaseTraceInfo(
        metadata={
            "parent_trace_context": {
                "parent_workflow_run_id": "outer-run",
                "parent_node_execution_id": "outer-run:tool-node",
            }
        }
    )

    assert trace_info.resolved_parent_context == ("outer-run", "outer-run:tool-node")
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_trace_context.py -q
```

Expected: fail with `ModuleNotFoundError: No module named 'core.ops.trace_context'` or missing property failures.

- [ ] **Step 3: Implement the helper**

Create `api/core/ops/trace_context.py`:

```python
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, StrictStr, ValidationError


class ParentTraceContext(BaseModel):
    """Private trace context propagated from an outer workflow tool node."""

    parent_workflow_run_id: StrictStr
    parent_node_execution_id: StrictStr

    model_config = ConfigDict(extra="forbid")


def parent_trace_context_from_metadata(metadata: Mapping[str, Any]) -> ParentTraceContext | None:
    raw_context = metadata.get("parent_trace_context")
    if isinstance(raw_context, ParentTraceContext):
        return raw_context
    if isinstance(raw_context, Mapping):
        try:
            return ParentTraceContext.model_validate(raw_context)
        except ValidationError:
            return None
    return None


def extract_parent_trace_context_from_args(args: Mapping[str, Any]) -> dict[str, ParentTraceContext]:
    raw_context = args.get("parent_trace_context")
    if isinstance(raw_context, ParentTraceContext):
        return {"parent_trace_context": raw_context}
    if isinstance(raw_context, Mapping):
        try:
            return {"parent_trace_context": ParentTraceContext.model_validate(raw_context)}
        except ValidationError:
            return {}
    return {}
```

Modify `api/core/ops/entities/trace_entity.py` by adding the import:

```python
from core.ops.trace_context import parent_trace_context_from_metadata
```

Add this property to `BaseTraceInfo` after `model_config`:

```python
    @property
    def resolved_trace_id(self) -> str | None:
        workflow_run_id = getattr(self, "workflow_run_id", None)
        if isinstance(workflow_run_id, str) and workflow_run_id:
            return workflow_run_id
        return str(self.message_id) if self.message_id else None

    @property
    def resolved_parent_context(self) -> tuple[str | None, str | None]:
        context = parent_trace_context_from_metadata(self.metadata)
        if context is None:
            return None, None
        return context.parent_workflow_run_id, context.parent_node_execution_id
```

- [ ] **Step 4: Run tests and verify they pass**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_trace_context.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add api/core/ops/trace_context.py api/core/ops/entities/trace_entity.py api/tests/unit_tests/core/ops/test_trace_context.py
git commit -m "feat: add ops parent trace context"
```

---

## Task 2: Workflow-as-Tool Parent Context Propagation

**Files:**
- Modify: `api/core/workflow/nodes/tool/tool_node.py`
- Modify: `api/core/tools/workflow_as_tool/tool.py`
- Modify: `api/core/app/apps/workflow/app_generator.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`
- Test: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`

- [ ] **Step 1: Add failing tests for tool node binding and workflow tool forwarding**

Append to `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`:

```python
from core.workflow.enums import SystemVariableKey
from core.variables.segments import StringSegment


class ParentTraceRecordingWorkflowTool(MockToolRuntime):
    parent_trace_context = None
    cleared = False

    def set_parent_trace_context(self, *, parent_workflow_run_id: str, parent_node_execution_id: str) -> None:
        self.parent_trace_context = {
            "parent_workflow_run_id": parent_workflow_run_id,
            "parent_node_execution_id": parent_node_execution_id,
        }

    def clear_parent_trace_context(self) -> None:
        self.cleared = True


def test_tool_node_attaches_parent_context_to_workflow_tool(monkeypatch: pytest.MonkeyPatch):
    tool_node = _create_tool_node()
    tool_node.graph_runtime_state.variable_pool.add(
        ["sys", SystemVariableKey.WORKFLOW_EXECUTION_ID.value],
        StringSegment(value="outer-run"),
    )
    tool_runtime = ParentTraceRecordingWorkflowTool()

    monkeypatch.setattr(
        "core.tools.tool_manager.ToolManager.get_workflow_tool_runtime",
        lambda *args, **kwargs: tool_runtime,
    )
    monkeypatch.setattr("core.tools.tool_engine.ToolEngine.generic_invoke", lambda *args, **kwargs: iter(()))

    list(tool_node._run())

    assert tool_runtime.parent_trace_context == {
        "parent_workflow_run_id": "outer-run",
        "parent_node_execution_id": "outer-run:1",
    }
```

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`:

```python
from core.ops.trace_context import ParentTraceContext


def test_workflow_tool_forwards_private_parent_trace_context(monkeypatch):
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="test_tool", invoke_from=InvokeFrom.EXPLORE)
    tool = WorkflowTool(
        workflow_app_id="",
        workflow_as_tool_id="",
        version="1",
        workflow_entities={},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )
    captured = {}

    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: object())
    monkeypatch.setattr(tool, "_get_workflow", lambda *args, **kwargs: object())
    monkeypatch.setattr(tool, "_transform_args", lambda tool_parameters: (tool_parameters, []))
    monkeypatch.setattr("flask_login.current_user", object())

    def fake_generate(**kwargs):
        captured.update(kwargs)
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    tool.set_parent_trace_context(
        parent_workflow_run_id="outer-run",
        parent_node_execution_id="outer-run:1",
    )

    list(tool.invoke("test-user", {"parent_trace_context": "user-input"}))

    assert captured["args"]["inputs"] == {"parent_trace_context": "user-input"}
    assert captured["args"]["parent_trace_context"] == ParentTraceContext(
        parent_workflow_run_id="outer-run",
        parent_node_execution_id="outer-run:1",
    )
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py::test_tool_node_attaches_parent_context_to_workflow_tool \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py::test_workflow_tool_forwards_private_parent_trace_context \
  -q
```

Expected: fail because `set_parent_trace_context` does not exist and tool node does not attach the context.

- [ ] **Step 3: Implement private propagation in `WorkflowTool`**

In `api/core/tools/workflow_as_tool/tool.py`, add:

```python
from core.ops.trace_context import ParentTraceContext, extract_parent_trace_context_from_args
```

Add the class attribute:

```python
    _parent_trace_context: ParentTraceContext | None
```

In `__init__`, before `super().__init__`:

```python
        self._parent_trace_context = None
```

Replace the `generator.generate(... args=...)` call with:

```python
        generator_args: dict[str, Any] = {"inputs": tool_parameters, "files": files}
        if self._parent_trace_context is not None:
            generator_args.update(
                extract_parent_trace_context_from_args({"parent_trace_context": self._parent_trace_context})
            )

        result = generator.generate(
            app_model=app,
            workflow=workflow,
            user=cast("Account | EndUser", current_user),
            args=generator_args,
            invoke_from=self.runtime.invoke_from,
            streaming=False,
            call_depth=self.workflow_call_depth + 1,
            workflow_thread_pool_id=self.thread_pool_id,
        )
```

Replace `fork_tool_runtime` with:

```python
    def fork_tool_runtime(self, runtime: ToolRuntime) -> "WorkflowTool":
        forked = self.__class__(
            entity=self.entity.model_copy(),
            runtime=runtime,
            workflow_app_id=self.workflow_app_id,
            workflow_as_tool_id=self.workflow_as_tool_id,
            workflow_entities=self.workflow_entities,
            workflow_call_depth=self.workflow_call_depth,
            version=self.version,
            label=self.label,
        )
        forked._parent_trace_context = self._parent_trace_context.model_copy() if self._parent_trace_context else None
        return forked
```

Add these methods:

```python
    def set_parent_trace_context(
        self,
        *,
        parent_workflow_run_id: str,
        parent_node_execution_id: str,
    ) -> None:
        self._parent_trace_context = ParentTraceContext(
            parent_workflow_run_id=parent_workflow_run_id,
            parent_node_execution_id=parent_node_execution_id,
        )

    def clear_parent_trace_context(self) -> None:
        self._parent_trace_context = None
```

- [ ] **Step 4: Implement tool node attachment**

In `api/core/workflow/nodes/tool/tool_node.py`, before `ToolEngine.generic_invoke(...)`, add:

```python
        workflow_run_var = self.graph_runtime_state.variable_pool.get(
            ["sys", SystemVariableKey.WORKFLOW_EXECUTION_ID.value]
        )
        outer_workflow_run_id = workflow_run_var.text if workflow_run_var else None
        if node_data.provider_type == ToolProviderType.WORKFLOW and outer_workflow_run_id:
            parent_node_execution_id = f"{outer_workflow_run_id}:{self.id}"
            if hasattr(tool_runtime, "set_parent_trace_context"):
                tool_runtime.set_parent_trace_context(
                    parent_workflow_run_id=outer_workflow_run_id,
                    parent_node_execution_id=parent_node_execution_id,
                )
        elif hasattr(tool_runtime, "clear_parent_trace_context"):
            tool_runtime.clear_parent_trace_context()
```

- [ ] **Step 5: Persist private context into workflow generate entity extras**

In `api/core/app/apps/workflow/app_generator.py`, add:

```python
from core.ops.trace_context import extract_parent_trace_context_from_args
```

Before `application_generate_entity = WorkflowAppGenerateEntity(...)`, add:

```python
        extras = {
            **extract_parent_trace_context_from_args(args),
        }
```

Pass it into the entity:

```python
            extras=extras,
```

- [ ] **Step 6: Run tests and verify they pass**

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py::test_tool_node_attaches_parent_context_to_workflow_tool \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py::test_workflow_tool_forwards_private_parent_trace_context \
  -q
```

Expected: both tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  api/core/workflow/nodes/tool/tool_node.py \
  api/core/tools/workflow_as_tool/tool.py \
  api/core/app/apps/workflow/app_generator.py \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py
git commit -m "feat: propagate workflow tool parent trace context"
```

---

## Task 3: Workflow Trace Metadata Persistence

**Files:**
- Modify: `api/core/ops/ops_trace_manager.py`
- Modify: `api/core/workflow/workflow_cycle_manager.py`
- Modify: `api/core/app/apps/workflow/generate_task_pipeline.py`
- Modify: `api/core/app/apps/advanced_chat/generate_task_pipeline.py`
- Test: `api/tests/unit_tests/core/ops/test_trace_context.py`

- [ ] **Step 1: Add failing test for `TraceTask.workflow_trace` metadata**

Append to `api/tests/unit_tests/core/ops/test_trace_context.py`:

```python
from types import SimpleNamespace

from core.ops.entities.trace_entity import TraceTaskName
from core.ops.ops_trace_manager import TraceTask
from core.ops.trace_context import ParentTraceContext


def test_trace_task_workflow_trace_keeps_parent_trace_context(monkeypatch):
    workflow_run = SimpleNamespace(
        id="child-run",
        workflow_id="workflow-id",
        tenant_id="tenant-id",
        elapsed_time=1.0,
        status="succeeded",
        inputs_dict={},
        outputs_dict={},
        version="1",
        error=None,
        total_tokens=0,
        app_id="app-id",
        triggered_from="workflow-run",
        created_at=None,
        finished_at=None,
        to_dict=lambda: {"id": "child-run"},
    )

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def scalars(self, stmt):
            return SimpleNamespace(first=lambda: workflow_run)

        def scalar(self, stmt):
            return None

    monkeypatch.setattr("core.ops.ops_trace_manager.Session", lambda *args, **kwargs: FakeSession())

    trace_task = TraceTask(
        TraceTaskName.WORKFLOW_TRACE,
        workflow_execution=SimpleNamespace(id_="child-run"),
        conversation_id=None,
        user_id="user-id",
        parent_trace_context=ParentTraceContext(
            parent_workflow_run_id="outer-run",
            parent_node_execution_id="outer-run:tool-node",
        ),
    )

    trace_info = trace_task.execute()

    assert trace_info.metadata["parent_trace_context"] == {
        "parent_workflow_run_id": "outer-run",
        "parent_node_execution_id": "outer-run:tool-node",
    }
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_trace_context.py::test_trace_task_workflow_trace_keeps_parent_trace_context -q
```

Expected: fail because `parent_trace_context` is not added to workflow trace metadata.

- [ ] **Step 3: Update `TraceTask.workflow_trace`**

In `api/core/ops/ops_trace_manager.py`, import:

```python
from core.ops.trace_context import ParentTraceContext
```

Inside `TraceTask.workflow_trace`, after building `metadata`, add:

```python
            parent_trace_context = self.kwargs.get("parent_trace_context")
            if isinstance(parent_trace_context, ParentTraceContext):
                metadata["parent_trace_context"] = parent_trace_context.model_dump()
            elif isinstance(parent_trace_context, dict):
                metadata["parent_trace_context"] = parent_trace_context
```

- [ ] **Step 4: Update workflow cycle manager to accept the context**

In `api/core/workflow/workflow_cycle_manager.py`, add a `parent_trace_context: ParentTraceContext | None = None` keyword argument to:

```python
    def handle_workflow_run_success(..., parent_trace_context: ParentTraceContext | None = None) -> WorkflowExecution:
```

```python
    def handle_workflow_run_partial_success(..., parent_trace_context: ParentTraceContext | None = None) -> WorkflowExecution:
```

```python
    def handle_workflow_run_failed(..., parent_trace_context: ParentTraceContext | None = None) -> WorkflowExecution:
```

Import:

```python
from core.ops.trace_context import ParentTraceContext
```

For each `TraceTask(TraceTaskName.WORKFLOW_TRACE, ...)` in those methods, add:

```python
                    parent_trace_context=parent_trace_context,
```

- [ ] **Step 5: Pass context from workflow and chatflow pipelines**

In both `api/core/app/apps/workflow/generate_task_pipeline.py` and `api/core/app/apps/advanced_chat/generate_task_pipeline.py`, before each call to `handle_workflow_run_success`, `handle_workflow_run_partial_success`, and `handle_workflow_run_failed`, add:

```python
                    parent_trace_context = self._application_generate_entity.extras.get("parent_trace_context")
```

Add this argument to each call:

```python
                        parent_trace_context=parent_trace_context,
```

- [ ] **Step 6: Run focused test**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_trace_context.py::test_trace_task_workflow_trace_keeps_parent_trace_context -q
```

Expected: pass.

- [ ] **Step 7: Commit**

```bash
git add \
  api/core/ops/ops_trace_manager.py \
  api/core/workflow/workflow_cycle_manager.py \
  api/core/app/apps/workflow/generate_task_pipeline.py \
  api/core/app/apps/advanced_chat/generate_task_pipeline.py \
  api/tests/unit_tests/core/ops/test_trace_context.py
git commit -m "feat: persist workflow parent trace metadata"
```

---

## Task 4: Retryable Ops Trace Dispatch

**Files:**
- Create: `api/core/ops/exceptions.py`
- Modify: `api/configs/feature/__init__.py`
- Modify: `api/.env.example`
- Modify: `docker/middleware.env.example`
- Modify: `api/tasks/ops_trace_task.py`
- Test: `api/tests/unit_tests/tasks/test_ops_trace_task.py`

- [ ] **Step 1: Write failing tests for retry behavior**

Create `api/tests/unit_tests/tasks/test_ops_trace_task.py`:

```python
import json
from types import SimpleNamespace

import pytest
from celery.exceptions import Retry

from core.ops.exceptions import RetryableTraceDispatchError
from tasks.ops_trace_task import process_trace_tasks


class RetryableProvider:
    def trace(self, trace_info):
        raise RetryableTraceDispatchError("parent span not ready")


class SuccessfulProvider:
    def trace(self, trace_info):
        return None


def _patch_payload(monkeypatch, provider, file_data):
    calls = {"deleted": False, "saved": None, "failed_count": 0}

    monkeypatch.setattr("tasks.ops_trace_task.storage.load", lambda path: json.dumps(file_data))
    monkeypatch.setattr("tasks.ops_trace_task.storage.delete", lambda path: calls.__setitem__("deleted", True))
    monkeypatch.setattr("tasks.ops_trace_task.storage.save", lambda path, data: calls.__setitem__("saved", data))
    monkeypatch.setattr("tasks.ops_trace_task.redis_client.incr", lambda key: calls.__setitem__("failed_count", 1))
    monkeypatch.setattr("core.ops.ops_trace_manager.OpsTraceManager.get_ops_trace_instance", lambda app_id: provider)
    return calls


def test_retryable_trace_dispatch_keeps_payload_when_retry_is_scheduled(monkeypatch):
    file_data = {"trace_info_type": "BaseTraceInfo", "trace_info": {"metadata": {}}}
    calls = _patch_payload(monkeypatch, RetryableProvider(), file_data)

    def fake_retry(exc, countdown):
        raise Retry()

    process_trace_tasks.request.retries = 0
    monkeypatch.setattr(process_trace_tasks, "retry", fake_retry)

    with pytest.raises(Retry):
        process_trace_tasks.run({"app_id": "app-id", "file_id": "file-id"})

    assert calls["deleted"] is False
    assert calls["failed_count"] == 0


def test_retryable_trace_dispatch_deletes_payload_after_budget_exhausted(monkeypatch):
    file_data = {"trace_info_type": "BaseTraceInfo", "trace_info": {"metadata": {}}}
    calls = _patch_payload(monkeypatch, RetryableProvider(), file_data)

    process_trace_tasks.request.retries = 60

    process_trace_tasks.run({"app_id": "app-id", "file_id": "file-id"})

    assert calls["deleted"] is True
    assert calls["failed_count"] == 1
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/tasks/test_ops_trace_task.py -q
```

Expected: fail because `core.ops.exceptions` and retry handling do not exist.

- [ ] **Step 3: Add retry exceptions**

Create `api/core/ops/exceptions.py`:

```python
"""Core exceptions shared by ops trace dispatchers and trace providers."""


class RetryableTraceDispatchError(RuntimeError):
    """Base class for transient trace dispatch failures that Celery may retry."""


class PendingTraceParentContextError(RetryableTraceDispatchError):
    """Raised when a nested trace arrives before its parent span context is available."""

    def __init__(self, parent_node_execution_id: str) -> None:
        self.parent_node_execution_id = parent_node_execution_id
        super().__init__(
            "Pending trace parent context for parent_node_execution_id="
            f"{parent_node_execution_id}. Retry after the parent span context is published."
        )
```

- [ ] **Step 4: Add config defaults**

In `api/configs/feature/__init__.py`, add:

```python
class OpsTraceConfig(BaseSettings):
    OPS_TRACE_RETRYABLE_DISPATCH_MAX_RETRIES: PositiveInt = Field(
        description="Maximum retry attempts for transient ops trace provider dispatch failures.",
        default=60,
    )

    OPS_TRACE_RETRYABLE_DISPATCH_DELAY_SECONDS: PositiveInt = Field(
        description="Delay in seconds between transient ops trace provider dispatch retry attempts.",
        default=5,
    )
```

Add `OpsTraceConfig` to the `FeatureConfig(...)` inheritance list.

In `api/.env.example` and `docker/middleware.env.example`, add:

```text
OPS_TRACE_RETRYABLE_DISPATCH_MAX_RETRIES=60
OPS_TRACE_RETRYABLE_DISPATCH_DELAY_SECONDS=5
```

- [ ] **Step 5: Update `ops_trace_task` retry behavior**

In `api/tasks/ops_trace_task.py`, add imports:

```python
from celery.exceptions import Retry

from configs import dify_config
from core.ops.exceptions import RetryableTraceDispatchError
```

Add module constants:

```python
logger = logging.getLogger(__name__)

_RETRYABLE_TRACE_DISPATCH_LIMIT = dify_config.OPS_TRACE_RETRYABLE_DISPATCH_MAX_RETRIES
_RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS = dify_config.OPS_TRACE_RETRYABLE_DISPATCH_DELAY_SECONDS
```

Replace the decorator and function signature:

```python
@shared_task(
    queue="ops_trace",
    bind=True,
    max_retries=_RETRYABLE_TRACE_DISPATCH_LIMIT,
    default_retry_delay=_RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS,
)
def process_trace_tasks(self, file_info):
```

Inside the function, set before `try`:

```python
    should_delete_file = True
```

Add this exception block before the generic `except Exception`:

```python
    except RetryableTraceDispatchError as e:
        if self.request.retries >= _RETRYABLE_TRACE_DISPATCH_LIMIT:
            logger.exception("Retryable trace dispatch budget exhausted, app_id: %s", app_id)
            failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
            redis_client.incr(failed_key)
        else:
            logger.warning(
                "Retryable trace dispatch failure, scheduling retry %s/%s for app_id %s: %s",
                self.request.retries + 1,
                _RETRYABLE_TRACE_DISPATCH_LIMIT,
                app_id,
                e,
            )
            try:
                raise self.retry(exc=e, countdown=_RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS)
            except Retry:
                should_delete_file = False
                raise
            except Exception:
                logger.exception("Failed to schedule trace dispatch retry, app_id: %s", app_id)
                failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
                redis_client.incr(failed_key)
```

Replace the `finally` body with:

```python
        if should_delete_file:
            storage.delete(file_path)
```

- [ ] **Step 6: Run retry tests**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/tasks/test_ops_trace_task.py -q
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  api/core/ops/exceptions.py \
  api/configs/feature/__init__.py \
  api/.env.example \
  docker/middleware.env.example \
  api/tasks/ops_trace_task.py \
  api/tests/unit_tests/tasks/test_ops_trace_task.py
git commit -m "feat: retry transient ops trace dispatch"
```

---

## Task 5: Phoenix Root, Session, Node Hierarchy, and Parent Span Bridge

**Files:**
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
- Test: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`

- [ ] **Step 1: Write helper-focused Phoenix tests**

Create `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`:

```python
import json

import pytest

from core.ops.arize_phoenix_trace.arize_phoenix_trace import (
    _build_parent_span_bridge_id,
    _phoenix_parent_span_redis_key,
    _resolve_published_parent_span_context,
    _resolve_session_id,
)
from core.ops.exceptions import PendingTraceParentContextError


def test_resolve_session_id_prefers_conversation_id():
    assert _resolve_session_id(
        conversation_id="conversation-id",
        workflow_run_id="child-run",
        parent_workflow_run_id="outer-run",
    ) == "conversation-id"


def test_resolve_session_id_uses_parent_workflow_for_nested_workflow():
    assert _resolve_session_id(
        conversation_id=None,
        workflow_run_id="child-run",
        parent_workflow_run_id="outer-run",
    ) == "outer-run"


def test_resolve_session_id_uses_workflow_run_for_top_level_workflow():
    assert _resolve_session_id(
        conversation_id=None,
        workflow_run_id="workflow-run",
        parent_workflow_run_id=None,
    ) == "workflow-run"


def test_build_parent_span_bridge_id_uses_workflow_run_and_node_id():
    assert _build_parent_span_bridge_id("outer-run", "tool-node") == "outer-run:tool-node"


def test_missing_parent_span_context_raises_retryable_error(monkeypatch):
    monkeypatch.setattr("core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get", lambda key: None)

    with pytest.raises(PendingTraceParentContextError):
        _resolve_published_parent_span_context("outer-run:tool-node")


def test_invalid_parent_span_context_rejected(monkeypatch):
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.redis_client.get",
        lambda key: json.dumps({"traceparent": "invalid"}),
    )

    with pytest.raises(ValueError):
        _resolve_published_parent_span_context("outer-run:tool-node")
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py -q
```

Expected: fail because helper functions do not exist.

- [ ] **Step 3: Add Phoenix helper imports and functions**

In `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`, add imports:

```python
import re
from collections.abc import Mapping

from opentelemetry.trace import get_current_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from core.ops.exceptions import PendingTraceParentContextError
from core.ops.trace_context import parent_trace_context_from_metadata
from extensions.ext_redis import redis_client
```

Add module constants and helpers:

```python
_PHOENIX_PARENT_SPAN_CONTEXT_TTL_SECONDS = 300
_TRACEPARENT_PATTERN = re.compile(
    r"^(?P<version>[0-9a-f]{2})-(?P<trace_id>[0-9a-f]{32})-(?P<span_id>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})$"
)


def _build_parent_span_bridge_id(parent_workflow_run_id: str, node_id: str) -> str:
    return f"{parent_workflow_run_id}:{node_id}"


def _phoenix_parent_span_redis_key(parent_node_execution_id: str) -> str:
    return f"trace:phoenix:parent_span:{parent_node_execution_id}"


def _resolve_session_id(
    *,
    conversation_id: str | None,
    workflow_run_id: str | None,
    parent_workflow_run_id: str | None,
) -> str:
    return conversation_id or parent_workflow_run_id or workflow_run_id or ""


def _publish_parent_span_context(parent_node_execution_id: str, carrier: Mapping[str, str]) -> None:
    redis_client.setex(
        _phoenix_parent_span_redis_key(parent_node_execution_id),
        _PHOENIX_PARENT_SPAN_CONTEXT_TTL_SECONDS,
        json.dumps(dict(carrier), ensure_ascii=False),
    )


def _resolve_published_parent_span_context(parent_node_execution_id: str) -> dict[str, str]:
    raw_carrier = redis_client.get(_phoenix_parent_span_redis_key(parent_node_execution_id))
    if raw_carrier is None:
        raise PendingTraceParentContextError(parent_node_execution_id)
    if isinstance(raw_carrier, bytes):
        raw_carrier = raw_carrier.decode("utf-8")

    carrier = json.loads(raw_carrier)
    if not isinstance(carrier, dict):
        raise ValueError(f"Phoenix parent span context must be a JSON object: {parent_node_execution_id}")

    normalized_carrier = {str(key): str(value) for key, value in carrier.items()}
    traceparent = normalized_carrier.get("traceparent")
    if not traceparent or _TRACEPARENT_PATTERN.fullmatch(traceparent) is None:
        raise ValueError(f"Phoenix parent span context has invalid traceparent: {parent_node_execution_id}")

    extracted_context = TraceContextTextMapPropagator().extract(carrier=normalized_carrier)
    extracted_span_context = get_current_span(extracted_context).get_span_context()
    if not extracted_span_context.is_valid or not extracted_span_context.is_remote:
        raise ValueError(f"Phoenix parent span context could not be restored: {parent_node_execution_id}")

    return normalized_carrier
```

- [ ] **Step 4: Extend workflow node query fields**

In `_get_workflow_nodes`, include these selected columns:

```python
                WorkflowNodeExecutionModel.workflow_run_id,
                WorkflowNodeExecutionModel.predecessor_node_id,
                WorkflowNodeExecutionModel.node_execution_id,
                WorkflowNodeExecutionModel.node_id,
                WorkflowNodeExecutionModel.error,
```

- [ ] **Step 5: Rework `workflow_trace` span context and session**

Inside `workflow_trace`, after metadata is built, add:

```python
        parent_context = parent_trace_context_from_metadata(trace_info.metadata)
        parent_workflow_run_id = parent_context.parent_workflow_run_id if parent_context else None
        parent_node_execution_id = parent_context.parent_node_execution_id if parent_context else None
        session_id = _resolve_session_id(
            conversation_id=trace_info.conversation_id,
            workflow_run_id=trace_info.workflow_run_id,
            parent_workflow_run_id=parent_workflow_run_id,
        )
```

Use:

```python
        trace_id_source = parent_workflow_run_id or trace_info.workflow_run_id or trace_info.message_id
        trace_id = uuid_to_trace_id(trace_id_source)
```

When starting the workflow span, set:

```python
                SpanAttributes.SESSION_ID: session_id,
```

If `parent_node_execution_id` is present, resolve the published parent carrier and use it as the parent context. The implementation should call:

```python
        if parent_node_execution_id:
            carrier = _resolve_published_parent_span_context(parent_node_execution_id)
            parent_otel_context = TraceContextTextMapPropagator().extract(carrier=carrier)
        else:
            parent_otel_context = trace.set_span_in_context(trace.NonRecordingSpan(context))
```

Pass `context=parent_otel_context` to `start_span`.

- [ ] **Step 6: Parent node spans under workflow span or predecessor span**

Inside the node loop:

```python
            node_spans_by_node_id: dict[str, Any] = {}
```

For each node:

```python
                node_parent_span = node_spans_by_node_id.get(node_execution.predecessor_node_id, workflow_span)
                node_context = trace.set_span_in_context(node_parent_span)
```

Pass:

```python
                    context=node_context,
```

After starting a node span:

```python
                node_spans_by_node_id[str(node_execution.node_id)] = node_span
```

For tool nodes, publish the bridge carrier:

```python
                if node_execution.node_type == "tool" and node_execution.workflow_run_id and node_execution.node_id:
                    carrier: dict[str, str] = {}
                    TraceContextTextMapPropagator().inject(carrier=carrier, context=trace.set_span_in_context(node_span))
                    _publish_parent_span_context(
                        _build_parent_span_bridge_id(str(node_execution.workflow_run_id), str(node_execution.node_id)),
                        carrier,
                    )
```

- [ ] **Step 7: Run Phoenix helper tests**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py -q
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py
git commit -m "feat: improve phoenix workflow trace hierarchy"
```

---

## Task 6: Focused Regression Run and Temporary Patch Commit Boundary

**Files:**
- Verify only; no code changes expected.

- [ ] **Step 1: Run focused test suite**

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/ops/test_trace_context.py \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py \
  api/tests/unit_tests/tasks/test_ops_trace_task.py \
  -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run lint check for touched backend files**

Run:

```bash
uv run --project api ruff check \
  api/core/ops/exceptions.py \
  api/core/ops/trace_context.py \
  api/core/ops/entities/trace_entity.py \
  api/core/ops/ops_trace_manager.py \
  api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py \
  api/core/app/apps/workflow/app_generator.py \
  api/core/app/apps/workflow/generate_task_pipeline.py \
  api/core/app/apps/advanced_chat/generate_task_pipeline.py \
  api/core/workflow/workflow_cycle_manager.py \
  api/core/workflow/nodes/tool/tool_node.py \
  api/core/tools/workflow_as_tool/tool.py \
  api/tasks/ops_trace_task.py
```

Expected: no lint errors.

- [ ] **Step 3: Verify docker middleware config still renders**

Run:

```bash
tmp="$(mktemp -d)"
sed 's#./middleware.env#./middleware.env.example#g' docker/docker-compose.middleware.yaml > "$tmp/docker-compose.middleware.yaml"
cp docker/middleware.env.example "$tmp/middleware.env.example"
mkdir -p "$tmp/ssrf_proxy"
cp docker/ssrf_proxy/squid.conf.template "$tmp/ssrf_proxy/squid.conf.template"
docker compose -f "$tmp/docker-compose.middleware.yaml" --env-file "$tmp/middleware.env.example" config --quiet
rm -rf "$tmp"
```

Expected: command exits with status `0`.

- [ ] **Step 4: Confirm final patch isolation**

Run:

```bash
git log --oneline --decorate 1.6.0..HEAD
git diff --name-only 1.6.0..HEAD
```

Expected:

- The Phoenix backport implementation is in one or more commits after the spec/local-dev commits.
- No migration files are listed.
- No frontend files are listed.

- [ ] **Step 5: Add final implementation summary commit only if verification caused file edits**

If verification required file edits, commit them:

```bash
git add \
  api/core/ops/exceptions.py \
  api/core/ops/trace_context.py \
  api/core/ops/entities/trace_entity.py \
  api/core/ops/ops_trace_manager.py \
  api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py \
  api/core/app/apps/workflow/app_generator.py \
  api/core/app/apps/workflow/generate_task_pipeline.py \
  api/core/app/apps/advanced_chat/generate_task_pipeline.py \
  api/core/workflow/workflow_cycle_manager.py \
  api/core/workflow/nodes/tool/tool_node.py \
  api/core/tools/workflow_as_tool/tool.py \
  api/tasks/ops_trace_task.py \
  api/tests/unit_tests/core/ops/test_trace_context.py \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py \
  api/tests/unit_tests/tasks/test_ops_trace_task.py
git commit -m "fix: stabilize phoenix tracing backport"
```

If verification required no file edits, do not create an empty commit.

---

## Manual Verification

After the focused tests pass, run a local Phoenix middleware stack:

```bash
cp docker/middleware.env.example docker/middleware.env
make dev-down
cd docker
docker compose -f docker-compose.middleware.yaml --env-file middleware.env -p dify-middlewares-dev up -d db redis sandbox ssrf_proxy weaviate phoenix
```

Open Phoenix at:

```text
http://localhost:6006
```

Configure a Phoenix trace provider in Dify with an endpoint that reaches the local Phoenix OTLP HTTP endpoint:

```text
http://localhost:6006
```

Run one workflow that calls another workflow as a tool. In Phoenix, verify:

- the top-level workflow has one stable root span
- `session.id` is the conversation id for chatflows and the workflow run id for plain workflows
- child workflow spans appear under the outer workflow tool node when the parent carrier is available
- retry logs appear only when child trace dispatch races ahead of parent tool span publication

---

## Revert Before Upgrade

Before upgrading this branch to `1.14.1` or newer, revert the temporary implementation commit range while keeping unrelated business commits:

```bash
git log --oneline 1.6.0..HEAD
git revert --no-commit fd7b12b48e..HEAD
git commit -m "revert: remove temporary phoenix 1.6 tracing backport"
```

The `fd7b12b48e..HEAD` range preserves the spec and local-dev support commits that already exist on this branch and reverts implementation commits created after them. If unrelated business commits are added after the implementation commits, inspect `git log --oneline 1.6.0..HEAD` and revert only the commits whose messages start with the Phoenix backport implementation messages from this plan.

Then upgrade to the target version that contains upstream PR `#35605`.
