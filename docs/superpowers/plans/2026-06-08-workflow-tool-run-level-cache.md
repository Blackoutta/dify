# Workflow Tool Run-Level Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce workflow-as-tool metadata DB queries by reusing cached app/workflow entities during invocation and reusing workflow-tool metadata prototypes within one live parent workflow run.

**Architecture:** Add a lightweight `WorkflowToolRuntimeCache` owned by `GraphRuntimeState`. `ToolNode` passes that cache into `ToolManager`; `ToolManager` stores workflow-tool metadata prototypes on cache miss and always returns forked invocation tools. `WorkflowTool._invoke()` uses attached `workflow_entities` when valid, with short-session fallback only for missing entity keys.

**Tech Stack:** Python 3.12, Pydantic v2, SQLAlchemy ORM, pytest, Dify workflow graph runtime.

---

## File Structure

- Create: `api/core/workflow/graph_engine/entities/workflow_tool_runtime_cache.py`
  - Owns `WorkflowToolRuntimeCacheKey` and `WorkflowToolRuntimeCache`.
  - Uses `Any` for cached tool prototypes to avoid import cycles from workflow runtime entities into tools.
- Modify: `api/core/workflow/graph_engine/entities/graph_runtime_state.py`
  - Adds `workflow_tool_runtime_cache` with `default_factory`.
- Modify: `api/core/workflow/graph_engine/entities/__init__.py`
  - Exports cache types if useful for tests/call sites.
- Modify: `api/core/tools/workflow_as_tool/tool.py`
  - Uses valid cached app/workflow entities in `_invoke()`.
  - Falls back only for missing keys.
  - Deep-copies entity metadata and shallow-copies `workflow_entities` dict in `fork_tool_runtime()`.
- Modify: `api/core/tools/tool_manager.py`
  - Adds optional `workflow_tool_runtime_cache` parameter to `get_tool_runtime()` and `get_workflow_tool_runtime()`.
  - Uses cache only for `ToolProviderType.WORKFLOW`.
- Modify: `api/core/workflow/nodes/tool/tool_node.py`
  - Passes `self.graph_runtime_state.workflow_tool_runtime_cache` to `ToolManager.get_workflow_tool_runtime()`.
- Modify: `api/core/workflow/nodes/iteration/iteration_node.py`
  - Passes parent cache into nested `GraphRuntimeState`.
- Modify: `api/core/workflow/nodes/loop/loop_node.py`
  - Passes parent cache into nested `GraphRuntimeState`.
- Modify: `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md`
  - Updates metadata query estimate after implementation.
- Test: `api/tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py`
- Test: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`
- Test: `api/tests/unit_tests/core/tools/test_tool_manager.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py`

---

### Task 1: Add GraphRuntimeState-owned workflow tool cache

**Files:**
- Create: `api/core/workflow/graph_engine/entities/workflow_tool_runtime_cache.py`
- Modify: `api/core/workflow/graph_engine/entities/graph_runtime_state.py`
- Modify: `api/core/workflow/graph_engine/entities/__init__.py`
- Test: `api/tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py`

- [ ] **Step 1: Write failing cache tests**

Create `api/tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py`:

```python
from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import (
    WorkflowToolRuntimeCache,
    WorkflowToolRuntimeCacheKey,
)


def _state() -> GraphRuntimeState:
    return GraphRuntimeState(variable_pool=VariablePool(system_variables={}, user_inputs={}), start_at=0)


def test_graph_runtime_state_gets_independent_workflow_tool_runtime_cache():
    first = _state()
    second = _state()

    assert isinstance(first.workflow_tool_runtime_cache, WorkflowToolRuntimeCache)
    assert isinstance(second.workflow_tool_runtime_cache, WorkflowToolRuntimeCache)
    assert first.workflow_tool_runtime_cache is not second.workflow_tool_runtime_cache

    key = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-1", tool_name="child")
    first.workflow_tool_runtime_cache.workflow_tools[key] = object()

    assert key in first.workflow_tool_runtime_cache.workflow_tools
    assert key not in second.workflow_tool_runtime_cache.workflow_tools


def test_workflow_tool_runtime_cache_key_separates_tenant_provider_and_tool():
    cache = WorkflowToolRuntimeCache()
    key = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-1", tool_name="child")
    other_tenant = WorkflowToolRuntimeCacheKey(tenant_id="tenant-2", provider_id="provider-1", tool_name="child")
    other_provider = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-2", tool_name="child")
    other_tool = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-1", tool_name="other")

    cached_tool = object()
    cache.workflow_tools[key] = cached_tool

    assert cache.workflow_tools[key] is cached_tool
    assert other_tenant not in cache.workflow_tools
    assert other_provider not in cache.workflow_tools
    assert other_tool not in cache.workflow_tools
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py -v
```

Expected: FAIL with `ModuleNotFoundError` or `AttributeError` because cache types/state field do not exist.

- [ ] **Step 3: Implement cache types**

Create `api/core/workflow/graph_engine/entities/workflow_tool_runtime_cache.py`:

```python
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True, slots=True)
class WorkflowToolRuntimeCacheKey:
    tenant_id: str
    provider_id: str
    tool_name: str


class WorkflowToolRuntimeCache(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_tools: dict[WorkflowToolRuntimeCacheKey, Any] = Field(default_factory=dict)
```

Modify `api/core/workflow/graph_engine/entities/graph_runtime_state.py`:

```python
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import WorkflowToolRuntimeCache
```

Add this field to `GraphRuntimeState`:

```python
    workflow_tool_runtime_cache: WorkflowToolRuntimeCache = Field(default_factory=WorkflowToolRuntimeCache)
    """workflow-as-tool metadata prototype cache scoped to this live workflow run"""
```

Modify `api/core/workflow/graph_engine/entities/__init__.py` so it imports and exports cache types:

```python
from .workflow_tool_runtime_cache import WorkflowToolRuntimeCache, WorkflowToolRuntimeCacheKey
```

Ensure `__all__` includes:

```python
"WorkflowToolRuntimeCache",
"WorkflowToolRuntimeCacheKey",
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py -v
```

Expected: `2 passed`.

- [ ] **Step 5: Commit**

```bash
git add \
  api/core/workflow/graph_engine/entities/workflow_tool_runtime_cache.py \
  api/core/workflow/graph_engine/entities/graph_runtime_state.py \
  api/core/workflow/graph_engine/entities/__init__.py \
  api/tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py
git commit -m "feat: add workflow tool runtime cache state"
```

---

### Task 2: Use cached workflow entities in WorkflowTool._invoke and protect forks

**Files:**
- Modify: `api/core/tools/workflow_as_tool/tool.py`
- Test: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`

- [ ] **Step 1: Write failing `_invoke()` entity reuse tests**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`:

```python
from core.tools.entities.tool_entities import ToolParameter


def test_workflow_tool_invoke_uses_cached_app_and_workflow_entities(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": app, "workflow": workflow}
    captured = {}

    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("app reloaded")))
    monkeypatch.setattr(
        tool,
        "_get_workflow",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("workflow reloaded")),
    )
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())

    def fake_generate(self, **kwargs):
        captured.update(kwargs)
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    list(tool.invoke("user-1", {"query": "hello"}))

    assert captured["app_model"] is app
    assert captured["workflow"] is workflow


def test_workflow_tool_invoke_loads_only_missing_workflow_entity(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": app}
    calls = {"app": 0, "workflow": 0}

    def fail_get_app(*args, **kwargs):
        calls["app"] += 1
        raise AssertionError("app should not reload")

    def fake_get_workflow(*args, **kwargs):
        calls["workflow"] += 1
        return workflow

    monkeypatch.setattr(tool, "_get_app", fail_get_app)
    monkeypatch.setattr(tool, "_get_workflow", fake_get_workflow)
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())
    monkeypatch.setattr(
        "core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate",
        lambda self, **kwargs: {"data": {"outputs": {"answer": "ok"}}},
    )

    list(tool.invoke("user-1", {}))

    assert calls == {"app": 0, "workflow": 1}


def test_workflow_tool_invoke_loads_only_missing_app_entity(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"workflow": workflow}
    calls = {"app": 0, "workflow": 0}

    def fake_get_app(*args, **kwargs):
        calls["app"] += 1
        return app

    def fail_get_workflow(*args, **kwargs):
        calls["workflow"] += 1
        raise AssertionError("workflow should not reload")

    monkeypatch.setattr(tool, "_get_app", fake_get_app)
    monkeypatch.setattr(tool, "_get_workflow", fail_get_workflow)
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())
    monkeypatch.setattr(
        "core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate",
        lambda self, **kwargs: {"data": {"outputs": {"answer": "ok"}}},
    )

    list(tool.invoke("user-1", {}))

    assert calls == {"app": 1, "workflow": 0}


def test_workflow_tool_invoke_rejects_invalid_present_cached_entities(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": None, "workflow": workflow}

    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no fallback")))
    monkeypatch.setattr(
        tool,
        "_get_workflow",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no fallback")),
    )

    with pytest.raises(ValueError, match="invalid cached workflow tool app"):
        list(tool.invoke("user-1", {}))
```

- [ ] **Step 2: Write failing fork pollution tests**

Append to the same test file:

```python

def test_workflow_tool_fork_deep_copies_entity_parameters_and_copies_workflow_entities_dict():
    parameter = ToolParameter(
        name="query",
        label=I18nObject(en_US="Query"),
        human_description=I18nObject(en_US="Query"),
        type=ToolParameter.ToolParameterType.STRING,
        form=ToolParameter.ToolParameterForm.LLM,
        llm_description="Query",
        required=True,
    )
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[parameter],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="tenant-1", invoke_from=InvokeFrom.EXPLORE)
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    prototype = WorkflowTool(
        workflow_app_id="app-1",
        workflow_as_tool_id="provider-1",
        version="1",
        workflow_entities={"app": app, "workflow": workflow},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )

    fork1 = prototype.fork_tool_runtime(runtime)
    fork2 = prototype.fork_tool_runtime(runtime)

    fork1.entity.parameters[0].name = "changed"
    fork1.workflow_entities["app"] = App(id="other-app", tenant_id="tenant-1", mode="workflow", name="Other")

    assert prototype.entity.parameters[0].name == "query"
    assert fork2.entity.parameters[0].name == "query"
    assert prototype.workflow_entities["app"] is app
    assert fork2.workflow_entities["app"] is app
    assert fork1.workflow_entities is not prototype.workflow_entities
```

- [ ] **Step 3: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_tool.py -v
```

Expected: at least the new cached entity tests fail because `_invoke()` reloads app/workflow, and the fork pollution test fails because `entity.model_copy()` is shallow and `workflow_entities` dict is shared.

- [ ] **Step 4: Implement `_resolve_app_and_workflow()` helper and fork protection**

Modify `api/core/tools/workflow_as_tool/tool.py`.

Add a helper method to `WorkflowTool`:

```python
    def _resolve_app_and_workflow(self) -> tuple[App, Workflow]:
        app_key_present = "app" in self.workflow_entities
        workflow_key_present = "workflow" in self.workflow_entities

        app = self.workflow_entities.get("app")
        workflow = self.workflow_entities.get("workflow")

        if app_key_present and not isinstance(app, App):
            raise ValueError("invalid cached workflow tool app")
        if workflow_key_present and not isinstance(workflow, Workflow):
            raise ValueError("invalid cached workflow tool workflow")

        if not app_key_present:
            app = self._get_app(app_id=self.workflow_app_id)
        if not workflow_key_present:
            workflow = self._get_workflow(app_id=self.workflow_app_id, version=self.version)

        assert isinstance(app, App)
        assert isinstance(workflow, Workflow)
        return app, workflow
```

Replace the first two lines inside `_invoke()`:

```python
        app = self._get_app(app_id=self.workflow_app_id)
        workflow = self._get_workflow(app_id=self.workflow_app_id, version=self.version)
```

with:

```python
        app, workflow = self._resolve_app_and_workflow()
```

In `fork_tool_runtime()`, replace:

```python
            entity=self.entity.model_copy(),
```

with:

```python
            entity=self.entity.model_copy(deep=True),
```

and replace:

```python
            workflow_entities=self.workflow_entities,
```

with:

```python
            workflow_entities=dict(self.workflow_entities),
```

- [ ] **Step 5: Run tests to verify they pass**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_tool.py -v
```

Expected: all workflow-as-tool tool tests pass.

- [ ] **Step 6: Commit**

```bash
git add api/core/tools/workflow_as_tool/tool.py api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py
git commit -m "fix: reuse cached workflow tool entities"
```

---

### Task 3: Add ToolManager workflow tool runtime cache support

**Files:**
- Modify: `api/core/tools/tool_manager.py`
- Test: `api/tests/unit_tests/core/tools/test_tool_manager.py`

- [ ] **Step 1: Write failing ToolManager cache tests**

Append to `api/tests/unit_tests/core/tools/test_tool_manager.py`:

```python
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import WorkflowToolRuntimeCache


def test_get_tool_runtime_workflow_uses_run_level_cache_on_second_call(monkeypatch):
    cache = WorkflowToolRuntimeCache()
    loads = {"count": 0}
    forked_runtime_ids = []

    class FakeWorkflowTool:
        workflow_app_id = "app-1"

        def fork_tool_runtime(self, runtime):
            forked_runtime_ids.append(id(runtime))
            return SimpleNamespace(runtime=runtime, workflow_app_id=self.workflow_app_id, prototype=self)

    class FakeController:
        def __init__(self):
            self.tool = FakeWorkflowTool()

        def get_tools(self, tenant_id):
            assert tenant_id == "tenant-1"
            return [self.tool]

    def fake_from_db_by_id(provider_id, *, tenant_id=None):
        assert provider_id == "provider-1"
        assert tenant_id == "tenant-1"
        loads["count"] += 1
        return FakeController()

    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.from_db_by_id", fake_from_db_by_id)

    first = ToolManager.get_tool_runtime(
        provider_type=ToolProviderType.WORKFLOW,
        provider_id="provider-1",
        tool_name="child_workflow",
        tenant_id="tenant-1",
        invoke_from=InvokeFrom.SERVICE_API,
        tool_invoke_from=ToolInvokeFrom.WORKFLOW,
        workflow_tool_runtime_cache=cache,
    )
    second = ToolManager.get_tool_runtime(
        provider_type=ToolProviderType.WORKFLOW,
        provider_id="provider-1",
        tool_name="child_workflow",
        tenant_id="tenant-1",
        invoke_from=InvokeFrom.SERVICE_API,
        tool_invoke_from=ToolInvokeFrom.WORKFLOW,
        workflow_tool_runtime_cache=cache,
    )

    assert loads["count"] == 1
    assert first.prototype is second.prototype
    assert first is not second
    assert first.runtime is not second.runtime
    assert len(set(forked_runtime_ids)) == 2


def test_get_tool_runtime_workflow_cache_is_scoped_by_cache_object_and_key(monkeypatch):
    cache1 = WorkflowToolRuntimeCache()
    cache2 = WorkflowToolRuntimeCache()
    loads = {"count": 0}

    class FakeWorkflowTool:
        workflow_app_id = "app-1"

        def __init__(self, label):
            self.label = label

        def fork_tool_runtime(self, runtime):
            return SimpleNamespace(runtime=runtime, label=self.label, workflow_app_id="app-1")

    class FakeController:
        def __init__(self, label):
            self.tool = FakeWorkflowTool(label)

        def get_tools(self, tenant_id):
            return [self.tool]

    def fake_from_db_by_id(provider_id, *, tenant_id=None):
        loads["count"] += 1
        return FakeController(f"{tenant_id}:{provider_id}:{loads['count']}")

    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.from_db_by_id", fake_from_db_by_id)

    a1 = ToolManager.get_tool_runtime(ToolProviderType.WORKFLOW, "provider-1", "child", "tenant-1", workflow_tool_runtime_cache=cache1)
    a2 = ToolManager.get_tool_runtime(ToolProviderType.WORKFLOW, "provider-1", "child", "tenant-1", workflow_tool_runtime_cache=cache1)
    b1 = ToolManager.get_tool_runtime(ToolProviderType.WORKFLOW, "provider-1", "child", "tenant-1", workflow_tool_runtime_cache=cache2)
    c1 = ToolManager.get_tool_runtime(ToolProviderType.WORKFLOW, "provider-2", "child", "tenant-1", workflow_tool_runtime_cache=cache1)
    d1 = ToolManager.get_tool_runtime(ToolProviderType.WORKFLOW, "provider-1", "child", "tenant-2", workflow_tool_runtime_cache=cache1)

    assert a1.label == a2.label
    assert b1.label != a1.label
    assert c1.label != a1.label
    assert d1.label != a1.label
    assert loads["count"] == 4
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/test_tool_manager.py -v
```

Expected: FAIL with `TypeError` because `workflow_tool_runtime_cache` is not accepted, or with repeated load count.

- [ ] **Step 3: Implement optional cache parameters and lookup**

Modify imports in `api/core/tools/tool_manager.py`:

```python
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import (
    WorkflowToolRuntimeCache,
    WorkflowToolRuntimeCacheKey,
)
```

Change `get_tool_runtime()` signature to include the optional keyword parameter after `tool_invoke_from`:

```python
        workflow_tool_runtime_cache: WorkflowToolRuntimeCache | None = None,
```

In the `ToolProviderType.WORKFLOW` branch, replace direct controller load with cache-aware code:

```python
            cache_key = WorkflowToolRuntimeCacheKey(
                tenant_id=tenant_id,
                provider_id=provider_id,
                tool_name=tool_name,
            )
            workflow_tool_prototype: WorkflowTool | None = None
            if workflow_tool_runtime_cache is not None:
                cached = workflow_tool_runtime_cache.workflow_tools.get(cache_key)
                if cached is not None:
                    if not isinstance(cached, WorkflowTool):
                        raise TypeError("cached workflow tool runtime prototype must be a WorkflowTool")
                    workflow_tool_prototype = cached

            if workflow_tool_prototype is None:
                try:
                    controller = WorkflowToolProviderController.from_db_by_id(provider_id, tenant_id=tenant_id)
                except ValueError as exc:
                    raise ToolProviderNotFoundError(f"workflow provider {provider_id} not found") from exc

                controller_tools: list[WorkflowTool] = controller.get_tools(tenant_id=tenant_id)
                if controller_tools is None or len(controller_tools) == 0:
                    raise ToolProviderNotFoundError(f"workflow provider {provider_id} not found")

                workflow_tool_prototype = controller_tools[0]
                if workflow_tool_runtime_cache is not None:
                    workflow_tool_runtime_cache.workflow_tools[cache_key] = workflow_tool_prototype

            return cast(
                WorkflowTool,
                workflow_tool_prototype.fork_tool_runtime(
                    runtime=ToolRuntime(
                        tenant_id=tenant_id,
                        credentials={},
                        invoke_from=invoke_from,
                        tool_invoke_from=tool_invoke_from,
                    )
                ),
            )
```

Change `get_workflow_tool_runtime()` signature to include:

```python
        workflow_tool_runtime_cache: WorkflowToolRuntimeCache | None = None,
```

Pass the cache into `get_tool_runtime()`:

```python
            workflow_tool_runtime_cache=workflow_tool_runtime_cache,
```

- [ ] **Step 4: Run ToolManager tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/test_tool_manager.py -v
```

Expected: all ToolManager tests pass.

- [ ] **Step 5: Commit**

```bash
git add api/core/tools/tool_manager.py api/tests/unit_tests/core/tools/test_tool_manager.py
git commit -m "feat: cache workflow tool runtime prototypes per run"
```

---

### Task 4: Wire cache through ToolNode and protect prototype invocation state

**Files:**
- Modify: `api/core/workflow/nodes/tool/tool_node.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`

- [ ] **Step 1: Write failing ToolNode wiring test**

Append to `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`:

```python

def test_tool_node_passes_workflow_tool_runtime_cache_to_tool_manager(monkeypatch: pytest.MonkeyPatch):
    tool_node = _create_tool_node()
    captured = {}
    tool_runtime = TraceSessionRecordingWorkflowTool()

    def fake_get_workflow_tool_runtime(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return tool_runtime

    monkeypatch.setattr("core.tools.tool_manager.ToolManager.get_workflow_tool_runtime", fake_get_workflow_tool_runtime)
    monkeypatch.setattr("core.tools.tool_engine.ToolEngine.generic_invoke", lambda *args, **kwargs: iter(()))

    list(tool_node._run())

    assert captured["kwargs"]["workflow_tool_runtime_cache"] is tool_node.graph_runtime_state.workflow_tool_runtime_cache
```

- [ ] **Step 2: Write failing prototype trace isolation test**

Append to the same test file:

```python

def test_tool_node_sets_trace_context_on_fork_not_cached_prototype(monkeypatch: pytest.MonkeyPatch):
    tool_node = _create_tool_node(trace_session_id="external-session")
    tool_node.graph_runtime_state.variable_pool.add(
        ["sys", SystemVariableKey.WORKFLOW_EXECUTION_ID.value],
        StringSegment(value="outer-run"),
    )
    prototype = TraceSessionRecordingWorkflowTool()

    class ForkedTraceTool(TraceSessionRecordingWorkflowTool):
        pass

    forked = ForkedTraceTool()

    def fake_get_workflow_tool_runtime(*args, **kwargs):
        return forked

    monkeypatch.setattr("core.tools.tool_manager.ToolManager.get_workflow_tool_runtime", fake_get_workflow_tool_runtime)
    monkeypatch.setattr("core.tools.tool_engine.ToolEngine.generic_invoke", lambda *args, **kwargs: iter(()))

    list(tool_node._run())

    assert forked.parent_trace_context == {
        "parent_workflow_run_id": "outer-run",
        "parent_node_execution_id": "outer-run:1",
    }
    assert forked.trace_session_id == "external-session"
    assert prototype.parent_trace_context is None
    assert prototype.trace_session_id is None
```

- [ ] **Step 3: Run ToolNode tests to verify the wiring test fails**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py -v
```

Expected: FAIL with `KeyError: 'workflow_tool_runtime_cache'` or equivalent because ToolNode does not pass the cache yet.

- [ ] **Step 4: Implement ToolNode cache wiring**

Modify `api/core/workflow/nodes/tool/tool_node.py`. Replace:

```python
            tool_runtime = ToolManager.get_workflow_tool_runtime(
                self.tenant_id, self.app_id, self.node_id, self.node_data, self.invoke_from, variable_pool
            )
```

with:

```python
            tool_runtime = ToolManager.get_workflow_tool_runtime(
                self.tenant_id,
                self.app_id,
                self.node_id,
                self.node_data,
                self.invoke_from,
                variable_pool,
                workflow_tool_runtime_cache=self.graph_runtime_state.workflow_tool_runtime_cache,
            )
```

- [ ] **Step 5: Run ToolNode tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py -v
```

Expected: all ToolNode tests pass.

- [ ] **Step 6: Commit**

```bash
git add api/core/workflow/nodes/tool/tool_node.py api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py
git commit -m "feat: pass workflow tool cache through tool node"
```

---

### Task 5: Propagate cache through iteration and loop subgraphs

**Files:**
- Modify: `api/core/workflow/nodes/iteration/iteration_node.py`
- Modify: `api/core/workflow/nodes/loop/loop_node.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py`

- [ ] **Step 1: Write failing iteration propagation test**

Append to `api/tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py`:

```python

def test_iteration_subgraph_reuses_parent_workflow_tool_runtime_cache(monkeypatch):
    captured = {}
    parent_state = GraphRuntimeState(
        variable_pool=VariablePool(system_variables={}, user_inputs={}),
        start_at=time.perf_counter(),
    )
    parent_state.variable_pool.add(["iteration-1", "items"], ["a"])

    graph_config = {
        "nodes": [
            {
                "id": "iteration-1",
                "data": {
                    "title": "iteration",
                    "type": "iteration",
                    "iterator_selector": ["iteration-1", "items"],
                    "output_selector": ["inner", "output"],
                    "output_type": "array[string]",
                    "start_node_id": "inner",
                    "startNodeType": "template-transform",
                },
            },
            {
                "id": "inner",
                "data": {
                    "title": "inner",
                    "type": "template-transform",
                    "iteration_id": "iteration-1",
                    "template": "{{ item }}",
                    "variables": [{"variable": "item", "value_selector": ["iteration-1", "item"]}],
                },
            },
        ],
        "edges": [],
    }
    graph = Graph.init(graph_config=graph_config)
    init_params = GraphInitParams(
        tenant_id="1",
        app_id="1",
        workflow_type=WorkflowType.CHAT,
        workflow_id="1",
        graph_config=graph_config,
        user_id="1",
        user_from=UserFrom.ACCOUNT,
        invoke_from=InvokeFrom.DEBUGGER,
        call_depth=0,
    )
    iteration_node = IterationNode(
        id="iteration-1",
        config=graph_config["nodes"][0],
        graph_init_params=init_params,
        graph=graph,
        graph_runtime_state=parent_state,
    )

    class FakeGraphEngine:
        def __init__(self, *args, **kwargs):
            captured["graph_runtime_state"] = kwargs["graph_runtime_state"]
            self.graph_runtime_state = kwargs["graph_runtime_state"]

        def run(self):
            return iter(())

    monkeypatch.setattr("core.workflow.nodes.iteration.iteration_node.GraphEngine", FakeGraphEngine)

    list(iteration_node._run())

    assert captured["graph_runtime_state"].workflow_tool_runtime_cache is parent_state.workflow_tool_runtime_cache
```

If this test conflicts with existing iteration imports or graph setup, keep the same assertion but adapt only constructor boilerplate to match existing test helpers in this file.

- [ ] **Step 2: Write failing loop propagation test**

Create `api/tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py` if it does not exist. Add a focused test that monkeypatches `GraphEngine` and asserts the nested state gets the parent cache:

```python
import time

from core.app.entities.app_invoke_entities import InvokeFrom
from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_init_params import GraphInitParams
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.nodes.loop.loop_node import LoopNode
from models.enums import UserFrom
from models.workflow import WorkflowType


def test_loop_subgraph_reuses_parent_workflow_tool_runtime_cache(monkeypatch):
    captured = {}
    parent_state = GraphRuntimeState(
        variable_pool=VariablePool(system_variables={}, user_inputs={}),
        start_at=time.perf_counter(),
    )

    graph_config = {
        "nodes": [
            {
                "id": "loop-1",
                "data": {
                    "title": "loop",
                    "type": "loop",
                    "start_node_id": "inner",
                    "startNodeType": "template-transform",
                    "break_conditions": [],
                    "loop_count": 1,
                    "logical_operator": "and",
                },
            },
            {
                "id": "inner",
                "data": {
                    "title": "inner",
                    "type": "template-transform",
                    "loop_id": "loop-1",
                    "template": "ok",
                    "variables": [],
                },
            },
        ],
        "edges": [],
    }
    graph = Graph.init(graph_config=graph_config)
    init_params = GraphInitParams(
        tenant_id="1",
        app_id="1",
        workflow_type=WorkflowType.CHAT,
        workflow_id="1",
        graph_config=graph_config,
        user_id="1",
        user_from=UserFrom.ACCOUNT,
        invoke_from=InvokeFrom.DEBUGGER,
        call_depth=0,
    )
    loop_node = LoopNode(
        id="loop-1",
        config=graph_config["nodes"][0],
        graph_init_params=init_params,
        graph=graph,
        graph_runtime_state=parent_state,
    )

    class FakeGraphEngine:
        def __init__(self, *args, **kwargs):
            captured["graph_runtime_state"] = kwargs["graph_runtime_state"]
            self.graph_runtime_state = kwargs["graph_runtime_state"]

        def run(self):
            return iter(())

    monkeypatch.setattr("core.workflow.nodes.loop.loop_node.GraphEngine", FakeGraphEngine)

    list(loop_node._run())

    assert captured["graph_runtime_state"].workflow_tool_runtime_cache is parent_state.workflow_tool_runtime_cache
```

- [ ] **Step 3: Run propagation tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' \
  tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py::test_iteration_subgraph_reuses_parent_workflow_tool_runtime_cache \
  tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py::test_loop_subgraph_reuses_parent_workflow_tool_runtime_cache \
  -v
```

Expected: FAIL because nested `GraphRuntimeState` instances get fresh cache objects.

- [ ] **Step 4: Implement iteration cache propagation**

In `api/core/workflow/nodes/iteration/iteration_node.py`, replace the nested state construction:

```python
        graph_runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())
```

with:

```python
        graph_runtime_state = GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=time.perf_counter(),
            workflow_tool_runtime_cache=self.graph_runtime_state.workflow_tool_runtime_cache,
        )
```

- [ ] **Step 5: Implement loop cache propagation**

In `api/core/workflow/nodes/loop/loop_node.py`, replace the nested state construction:

```python
        graph_runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())
```

with:

```python
        graph_runtime_state = GraphRuntimeState(
            variable_pool=variable_pool,
            start_at=time.perf_counter(),
            workflow_tool_runtime_cache=self.graph_runtime_state.workflow_tool_runtime_cache,
        )
```

- [ ] **Step 6: Run propagation tests**

Run:

```bash
cd api && uv run pytest -o addopts='' \
  tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py::test_iteration_subgraph_reuses_parent_workflow_tool_runtime_cache \
  tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py::test_loop_subgraph_reuses_parent_workflow_tool_runtime_cache \
  -v
```

Expected: both tests pass.

- [ ] **Step 7: Commit**

```bash
git add \
  api/core/workflow/nodes/iteration/iteration_node.py \
  api/core/workflow/nodes/loop/loop_node.py \
  api/tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py \
  api/tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py
git commit -m "feat: share workflow tool cache in iteration and loop"
```

---

### Task 6: Verify ordinary child workflow runs keep independent cache

**Files:**
- Test: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`
- Modify only if needed: `api/core/tools/workflow_as_tool/tool.py`

- [ ] **Step 1: Write child-cache isolation test**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`:

```python

def test_workflow_tool_child_generation_does_not_receive_parent_workflow_tool_cache(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": app, "workflow": workflow}
    captured = {}

    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())

    def fake_generate(self, **kwargs):
        captured.update(kwargs)
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    list(tool.invoke("user-1", {}))

    assert "workflow_tool_runtime_cache" not in captured
    assert "workflow_tool_runtime_cache" not in captured["args"]
```

- [ ] **Step 2: Run the test**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_tool.py::test_workflow_tool_child_generation_does_not_receive_parent_workflow_tool_cache -v
```

Expected: PASS if no parent cache is passed to child generation. If it fails, remove any cache propagation from `WorkflowTool._invoke()` into `WorkflowAppGenerator.generate()`.

- [ ] **Step 3: Commit if test was added**

```bash
git add api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py api/core/tools/workflow_as_tool/tool.py
git commit -m "test: assert child workflow cache isolation"
```

---

### Task 7: Update sync DB call documentation and run focused verification

**Files:**
- Modify: `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md`

- [ ] **Step 1: Update metadata SELECT estimate documentation**

In `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md`, update the workflow-as-tool metadata section to reflect the new optimized path:

```text
First workflow-as-tool call in one parent workflow run:
  ToolManager / WorkflowToolProviderController.from_db_by_id(...):
    SELECT tool_workflow_providers by tenant_id/provider_id
    SELECT apps by app_id
    SELECT accounts by user_id
    SELECT workflows by app_id/version

  WorkflowTool._invoke(...):
    uses workflow_entities["app"] and workflow_entities["workflow"]
    no duplicate app/workflow SELECTs

Repeated same workflow tool in the same parent workflow run:
  cache hit on GraphRuntimeState.workflow_tool_runtime_cache
  no provider/app/account/workflow metadata SELECTs
```

Update rough count to:

```text
first call metadata SELECTs:       ~4
same-run cache-hit SELECTs:        ~0
```

Keep the note that `workflow_runs` remain synchronous and node execution DB writes remain visible.

- [ ] **Step 2: Run focused unit tests**

Run:

```bash
cd api && uv run pytest -o addopts='' \
  tests/unit_tests/core/workflow/graph_engine/entities/test_workflow_tool_runtime_cache.py \
  tests/unit_tests/core/tools/test_tool_manager.py \
  tests/unit_tests/core/tools/workflow_as_tool \
  tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py \
  tests/unit_tests/core/workflow/nodes/iteration/test_iteration.py::test_iteration_subgraph_reuses_parent_workflow_tool_runtime_cache \
  tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py::test_loop_subgraph_reuses_parent_workflow_tool_runtime_cache \
  -v
```

Expected: all selected tests pass.

- [ ] **Step 3: Run broader tools tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools -v
```

Expected: all core tool unit tests pass.

- [ ] **Step 4: Run import smoke**

Run:

```bash
cd api && uv run python - <<'PY'
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import WorkflowToolRuntimeCache, WorkflowToolRuntimeCacheKey
from core.tools.tool_manager import ToolManager
from core.tools.workflow_as_tool.tool import WorkflowTool
print('ok')
PY
```

Expected: prints `ok`.

- [ ] **Step 5: Commit docs and any final test adjustments**

```bash
git add docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md api/tests api/core
git commit -m "docs: update workflow tool metadata query estimates"
```

- [ ] **Step 6: Pressure-test verification after API restart**

Restart API/gunicorn so it loads the implementation. Run the existing load test:

```bash
make load-test-dify-workflow
```

During the run, use the `dify-postgres-load-test-diagnostics` skill. Minimum DB checks:

```bash
docker exec dify-middlewares-dev-db-1 sh -lc "psql -U \${POSTGRES_USER:-postgres} -d \${POSTGRES_DB:-dify} -P pager=off -c \"
select state, left(query, 120) as query_prefix, count(*)
from pg_stat_activity
where datname=current_database()
group by state, left(query, 120)
order by count(*) desc
limit 20;
\""
```

and repeated age samples:

```bash
for i in 1 2 3 4 5; do
  echo "--- sample $i $(date +%H:%M:%S) ---"
  docker exec dify-middlewares-dev-db-1 sh -lc "psql -U \${POSTGRES_USER:-postgres} -d \${POSTGRES_DB:-dify} -P pager=off -c \"
  select count(*) filter (where state='idle in transaction') as idle_tx,
         count(*) filter (where state='active') as active,
         count(*) filter (where state='idle') as idle,
         max(now()-xact_start) filter (where state='idle in transaction') as max_idle_tx_age
  from pg_stat_activity
  where datname=current_database();
  \""
  sleep 2
done
```

Expected:

- No long-lived `idle in transaction | SELECT tool_workflow_providers...` cluster.
- No recent `workflow_runs` failures.
- `tool_workflow_providers` snapshots are lower than the current post-session-fix baseline for workflows that repeatedly call the same workflow tool in one parent run.

- [ ] **Step 7: Commit pressure-test notes if documented**

If pressure-test results are summarized in a doc, commit them:

```bash
git add docs/superpowers/specs docs/superpowers/plans
git commit -m "docs: record workflow tool cache pressure results"
```

Skip this commit if no new documentation file is created or modified.

---

## Plan Self-Review

- Spec coverage:
  - Phase 1 cached app/workflow reuse: Task 2.
  - Present-invalid fail and partial-missing fallback: Task 2.
  - Deep copy entity and shallow copy `workflow_entities` dict: Task 2.
  - GraphRuntimeState-owned cache with no global registry: Task 1 and Task 3.
  - ToolManager cache lookup/miss/hit and key isolation: Task 3.
  - ToolNode wiring: Task 4.
  - Iteration/loop propagation: Task 5.
  - Ordinary child workflow cache isolation: Task 6.
  - Documentation and pressure verification: Task 7.
- Placeholder scan: no unresolved placeholders; each implementation step includes concrete code or command.
- Type consistency:
  - Cache type names are `WorkflowToolRuntimeCache` and `WorkflowToolRuntimeCacheKey` throughout.
  - Optional parameter name is `workflow_tool_runtime_cache` throughout.
  - Cache key fields are `tenant_id`, `provider_id`, and `tool_name` throughout.
