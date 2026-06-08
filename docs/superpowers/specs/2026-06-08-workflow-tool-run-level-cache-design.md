# Workflow-as-Tool Run-Level Metadata Cache Design

## Context

The current branch has already fixed the main workflow-as-tool database session lifetime issue. During pressure tests, PostgreSQL no longer shows long-lived `idle in transaction | SELECT tool_workflow_providers...` sessions. Remaining pressure is mostly high-frequency short metadata queries plus synchronous `workflow_runs` and `workflow_node_executions` writes.

Current workflow-as-tool runtime construction path:

```text
ToolNode._run
  -> ToolManager.get_workflow_tool_runtime(...)
     -> ToolManager.get_tool_runtime(... WORKFLOW ...)
        -> WorkflowToolProviderController.from_db_by_id(provider_id, tenant_id)
           -> SELECT tool_workflow_providers
           -> SELECT apps
           -> SELECT accounts
           -> SELECT workflows
        -> controller.get_tools(...) returns prebuilt self.tools
        -> WorkflowTool.fork_tool_runtime(ToolRuntime(...))
  -> ToolEngine.generic_invoke(...)
     -> WorkflowTool._invoke(...)
        -> SELECT apps
        -> SELECT workflows
        -> WorkflowAppGenerator.generate(child)
```

The current first workflow-as-tool invocation therefore performs about six metadata SELECTs before the child workflow starts:

```text
provider/app/account/workflow construction: 4 SELECTs
invoke app/workflow reload:                 2 SELECTs
```

The session lifetime is now safe, but this metadata path is still visible under `hey -c 50` pressure as many short `idle in transaction` samples.

## Goals

1. Eliminate the duplicate app/workflow reload in `WorkflowTool._invoke()` when the tool already carries `workflow_entities`.
2. Add a run-level cache so repeated calls to the same workflow tool within one parent graph run reuse the already-built workflow-tool metadata prototype.
3. Keep cache scope local to one parent workflow run. Do not add process-global or cross-request caching.
4. Preserve correctness for runtime parameters, parent trace context, trace session id, call depth, thread pool id, and per-node invocation state.
5. Preserve fail-fast behavior for missing provider/app/workflow metadata.
6. Keep changes backward-compatible for callers that construct `WorkflowTool` without cached entities.

## Non-Goals

- Do not cache `workflow_runs` or `workflow_node_executions` writes.
- Do not add a process-global provider/workflow cache.
- Do not add cache invalidation for workflow/provider updates across requests.
- Do not change ActiveMQ behavior.
- Do not change workflow execution semantics or child workflow outputs.

## Proposed Design

### Phase 1: Reuse `workflow_entities` in `WorkflowTool._invoke()`

`WorkflowToolProviderController._get_db_provider_tool()` already attaches detached ORM objects to the tool:

```python
workflow_entities={
    "app": app,
    "workflow": workflow,
}
```

`WorkflowTool._invoke()` should use those entities when present and only fall back to `_get_app()` / `_get_workflow()` when the entity keys are absent:

```text
if workflow_entities lacks either "app" or "workflow":
  load the missing data with short sessions as today
elif workflow_entities["app"] is an App and workflow_entities["workflow"] is a Workflow:
  use them
else:
  fail immediately; do not silently fall back
```

The implementation should use `isinstance(app, App)` and `isinstance(workflow, Workflow)`, or an equivalent minimal type/attribute validation. A missing key preserves backward compatibility with older or test-built `WorkflowTool` objects. A present-but-`None` or present-but-wrong-type value indicates a malformed cached prototype and should fail rather than hiding cache corruption with extra DB reads.

This removes two duplicate SELECTs per workflow-as-tool invocation while preserving compatibility with callers that do not attach `workflow_entities`.

Acceptance criteria:

- A tool with valid `workflow_entities["app"]` and `workflow_entities["workflow"]` invokes the child generator without calling `_get_app()` or `_get_workflow()`.
- A tool missing either cached entity key still loads app/workflow via existing short-session methods.
- A tool with present-but-invalid cached entity values fails without fallback.
- Cached app/workflow are not mutated by `_invoke()`.

### Phase 2: Add run-level workflow tool runtime cache

Add a dedicated cache object and attach it to `GraphRuntimeState`:

```python
class WorkflowToolRuntimeCache(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    workflow_tools: dict[WorkflowToolRuntimeCacheKey, Any] = Field(default_factory=dict)
```

Avoid creating an import cycle from low-level workflow runtime entities back into the tools package. The cache type can live in a lightweight workflow runtime module and store values as `Any`, or use `TYPE_CHECKING` for `WorkflowTool` annotations. Runtime code that reads from the cache can validate/cast at the ToolManager boundary.

The exact implementation can use a frozen dataclass, tuple key, or typed string key. The key must include:

```text
tenant_id
provider_id
tool_name
```

`provider_type` can be implicit because this cache is workflow-tool only, but including it is acceptable if it keeps call sites clearer.

`GraphRuntimeState` gains:

```python
workflow_tool_runtime_cache: WorkflowToolRuntimeCache = Field(default_factory=WorkflowToolRuntimeCache)
```

This scope is intentionally one live parent workflow run. A new parent workflow run creates a new `GraphRuntimeState` and therefore a new cache object owned by that runtime state.

Do not implement this as a process-level `workflow_run_id -> cache` registry. A global registry would require cleanup, can leak memory, and risks cross-request contamination. The boundary is object ownership, not global lookup:

```text
live parent workflow run GraphRuntimeState owns cache
iteration/loop nested GraphRuntimeState receives same cache object
ordinary child workflow run creates its own cache with its own GraphRuntimeState
```

Important propagation rule:

- Ordinary child workflow runs invoked through workflow-as-tool must get their own cache and must not inherit the parent cache.
- Iteration and loop subgraphs that are still part of the same parent workflow run should inherit the parent `workflow_tool_runtime_cache` when they construct a nested `GraphRuntimeState`.

Current `iteration_node.py` and `loop_node.py` construct new `GraphRuntimeState(variable_pool=..., start_at=...)`. Implementation must pass through the parent cache there, otherwise workflow tools repeated inside iteration/loop subgraphs will miss the run-level cache even though they are part of the same parent workflow run.

### Cache data model

Cache values are **metadata prototypes**, not invocation instances.

Safe to cache:

- `WorkflowTool.entity`
- tool parameter schema
- `workflow_app_id`
- `workflow_as_tool_id`
- `version`
- `label`
- `workflow_entities["app"]`
- `workflow_entities["workflow"]`

Do not cache as shared mutable invocation state:

- `ToolRuntime` for the current invocation
- runtime parameters generated from node inputs
- parent trace context
- trace session id
- parent node execution id
- per-invocation files or transformed inputs

On cache hit, the caller must still call `fork_tool_runtime(ToolRuntime(...))`. `fork_tool_runtime()` must continue to create an invocation-local `WorkflowTool` object. If cached `workflow_entities` are reused by reference, `_invoke()` must treat app/workflow as read-only.

`WorkflowTool.fork_tool_runtime()` must deep-copy tool metadata and copy the `workflow_entities` container to avoid prototype pollution:

```python
entity=self.entity.model_copy(deep=True)
workflow_entities=dict(self.workflow_entities)
```

The current shallow `model_copy()` can share nested `ToolParameter` objects. Once a `WorkflowTool` becomes a longer-lived cache prototype, mutating parameters on one fork could affect later forks unless the entity is deep-copied.

`workflow_entities=dict(self.workflow_entities)` intentionally keeps the detached `App` and `Workflow` object references shared as read-only metadata, but prevents accidental key-level mutation on a fork from changing the cached prototype's dict.

### Call flow with cache

New flow:

```text
ToolNode._run
  -> ToolManager.get_workflow_tool_runtime(..., workflow_tool_runtime_cache=graph_runtime_state.workflow_tool_runtime_cache)
     -> ToolManager.get_tool_runtime(... WORKFLOW ..., workflow_tool_runtime_cache=cache)
        -> cache lookup by tenant_id/provider_id/tool_name
        -> miss: WorkflowToolProviderController.from_db_by_id(...), then store prototype
        -> hit: reuse cached prototype
        -> prototype.fork_tool_runtime(ToolRuntime(...))
  -> WorkflowTool._invoke(...)
     -> use workflow_entities app/workflow if present
     -> WorkflowAppGenerator.generate(child)
```

Expected metadata SELECT count:

```text
First workflow-as-tool call in parent run:
  provider/app/account/workflow construction: 4 SELECTs
  invoke app/workflow reload:                 0 SELECTs

Repeated same workflow tool in same parent run:
  metadata construction:                       0 SELECTs
  invoke app/workflow reload:                 0 SELECTs
```

## API and Compatibility

Add optional parameters rather than breaking existing callers:

```python
ToolManager.get_workflow_tool_runtime(..., workflow_tool_runtime_cache: WorkflowToolRuntimeCache | None = None)
ToolManager.get_tool_runtime(..., workflow_tool_runtime_cache: WorkflowToolRuntimeCache | None = None)
```

Only workflow-provider paths use the cache. Built-in, API, plugin, MCP, and agent tool behavior remains unchanged.

Existing callers that do not pass a cache continue to load metadata as today, except they still benefit from Phase 1 if the returned `WorkflowTool` carries `workflow_entities`.

## Concurrency and Isolation

The cache lives on `GraphRuntimeState`, which is already shared across node execution for a graph run. Parallel branches within the same graph run may access the cache concurrently.

Implementation should avoid corrupting cache state under concurrent misses. Acceptable strategies:

1. Simple dict with idempotent duplicate loads on race. This is acceptable because duplicate misses are safe and short-lived.
2. Add a small lock around cache writes if tests or runtime behavior show concurrent mutation risk.

Recommendation: start with the simple dict. The cached value is immutable-by-convention metadata, and duplicate DB loads on a race are acceptable. Do not hold DB sessions while waiting on cache locks.

Cache isolation requirements:

- Different parent workflow runs must not share cached tools.
- An ordinary child workflow run must not see the parent run cache unless future work explicitly passes it.
- Iteration and loop subgraphs within the same parent workflow run must share the parent cache.
- Different tenants must never share cache entries.
- Different provider ids or tool names must not collide.

## Error Handling

- Cache miss load errors should behave exactly like current metadata load errors.
- Do not cache failures or negative lookups.
- If cached prototype is malformed, fail normally rather than falling back silently, because malformed cache indicates a programming error.
- `WorkflowTool._invoke()` fallback to DB should only apply when entity keys are absent, not when keys are present-but-`None` or present-but-wrong-type.

## Testing Strategy

### Unit tests for Phase 1

File: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`

Add/adjust tests:

1. `WorkflowTool._invoke()` uses `workflow_entities["app"]` and `workflow_entities["workflow"]` without calling `_get_app()` / `_get_workflow()`.
2. `WorkflowTool._invoke()` falls back to `_get_app()` / `_get_workflow()` when `workflow_entities` is empty.
3. Partial missing entities load only the missing side:
   - `{"app": app}` loads workflow only.
   - `{"workflow": workflow}` loads app only.
4. Present-but-invalid `workflow_entities` values fail without DB fallback, for example `{"app": None, "workflow": workflow}`.
5. `fork_tool_runtime()` preserves `workflow_entities` while keeping trace context invocation-local.
6. `fork_tool_runtime()` deep-copies `entity`, so mutating forked tool parameters does not affect the cached prototype or another fork.
7. `fork_tool_runtime()` shallow-copies the `workflow_entities` dict container, so mutating keys on one fork does not affect the cached prototype or another fork.

### Unit tests for run-level cache object

File: `api/tests/unit_tests/core/workflow/graph_engine/entities/test_graph_runtime_state.py` or equivalent.

Tests:

1. New `GraphRuntimeState` instances get independent `workflow_tool_runtime_cache` objects.
2. Cache can store/retrieve workflow tool prototypes by tenant/provider/tool key.

### Unit tests for ToolManager cache use

File: `api/tests/unit_tests/core/tools/test_tool_manager.py`

Tests:

1. First workflow tool runtime call with cache misses and calls `WorkflowToolProviderController.from_db_by_id()` once.
2. Second call with the same cache and same key does not call `from_db_by_id()` again.
3. The returned runtime is forked each time, so runtime invoke metadata does not leak.
4. Different cache objects do not share entries.
5. Different tenant/provider/tool key misses independently.

### Unit tests for ToolNode wiring

File: `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`

Tests:

1. `ToolNode._run()` passes `graph_runtime_state.workflow_tool_runtime_cache` to `ToolManager.get_workflow_tool_runtime()`.
2. Existing parent trace context and trace session id behavior remains unchanged.
3. Cache hit returns a forked invocation whose `_parent_trace_context` and `_trace_session_id` are set only on the fork, while the cached prototype remains unset.

### Unit tests for iteration/loop cache propagation

Files:

- `api/tests/unit_tests/core/workflow/nodes/iteration/test_iteration_node.py` or nearest existing iteration-node test file.
- `api/tests/unit_tests/core/workflow/nodes/loop/test_loop_node.py` or nearest existing loop-node test file.

Tests:

1. Iteration subgraph `GraphRuntimeState` receives the same `workflow_tool_runtime_cache` object as the parent runtime state.
2. Loop subgraph `GraphRuntimeState` receives the same `workflow_tool_runtime_cache` object as the parent runtime state.
3. Ordinary workflow-as-tool child workflow generation does not inherit the parent cache.

### Pressure-test verification

After implementation and API restart, run the same `hey -n 10000 -c 50` workflow-as-tool pressure test.

Expected DB observations:

- No long-lived `idle in transaction | SELECT tool_workflow_providers...` cluster.
- `tool_workflow_providers` query count should drop compared with current post-fix baseline, especially for parent runs with repeated same workflow tool.
- `workflow_runs` and `workflow_node_executions` synchronous writes remain visible.
- Recent `workflow_runs` should not show `QueuePool limit...timeout` or application errors.

Use the `dify-postgres-load-test-diagnostics` skill for sampling.

## Risks and Mitigations

### Stale metadata within one run

If a workflow provider or child workflow is edited while a parent workflow run is in progress, the run-level cache may keep using metadata loaded earlier in the same run.

Mitigation: acceptable. Workflow execution should be internally consistent during one run. Cross-run freshness is preserved because the cache is not process-global.

### Mutable cached prototype

If invocation-specific state is written onto the cached `WorkflowTool`, one call can affect another.

Mitigation: only cache the prototype; always return `fork_tool_runtime(...)`. Tests must prove trace context and runtime metadata do not leak.

### Parallel branch races

Two parallel branches may miss the cache at the same time and both load metadata.

Mitigation: acceptable for the first implementation. Both store equivalent prototypes. Avoid locking around DB calls.

### Detached ORM object safety

Cached `App` and `Workflow` objects are detached after short-session load. They must only be read by child generation.

Mitigation: existing short-session fix already stores detached app/workflow on `WorkflowTool.workflow_entities`; Phase 1 simply uses them instead of reloading.

## Documentation Updates

Update `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md` after implementation to reflect the new metadata SELECT estimates:

```text
first call after optimization: ~4 metadata SELECTs
same-run cache hit:           ~0 metadata SELECTs
```

## Acceptance Criteria

- Focused unit tests pass.
- `tests/unit_tests/core/tools`, workflow-as-tool tests, and tool node tests pass.
- First workflow-as-tool call no longer performs duplicate app/workflow reload inside `_invoke()` when entities are present.
- Repeated same workflow tool calls in one parent workflow run avoid rebuilding provider/app/account/workflow metadata, including calls inside iteration/loop subgraphs.
- Different parent `GraphRuntimeState` instances do not share cache.
- There is no process-global `workflow_run_id -> cache` registry.
- Ordinary child workflow runs do not inherit parent workflow tool runtime cache.
- No runtime parameter, trace context, trace session id, parent node execution id, mutated parameter schema, or mutated `workflow_entities` dict keys leak between cached invocations.
- Load test shows no long-lived `tool_workflow_providers` idle transaction cluster and no new workflow failures.
