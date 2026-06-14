# Workflow Tool Provider Redis Metadata Cache Design

## Goal

Reduce database pressure during high-concurrency workflow-as-tool execution by adding a cross-request Redis read-through cache for workflow tool provider metadata.

This complements the existing per-run workflow tool runtime cache. The per-run cache avoids repeated provider resolution inside one parent workflow run; this Redis cache avoids repeated provider/app/workflow/account metadata loads across many concurrent parent workflow runs that call the same workflow tool for the first time.

## Problem

Nested workflow load tests show high concurrent access to the workflow-as-tool metadata path:

```text
ToolNode._run
  -> ToolManager.get_workflow_tool_runtime(...)
      -> WorkflowToolProviderController.from_db_by_id(...)
          -> SELECT tool_workflow_providers...
          -> SELECT apps...
          -> SELECT accounts...
          -> SELECT workflows...
```

Short-lived SQLAlchemy sessions and the per-run cache already reduce transaction lifetime and repeated lookups inside a single run. They do not prevent a first-call stampede where many parent workflow runs concurrently resolve the same workflow tool.

Example:

```text
hey -c 100 ...
100 parent workflow runs
all first-call the same wf2_tool
all miss per-run cache
all hit DB for the same metadata
```

## Non-Goals

- Do not replace the existing per-run `WorkflowToolRuntimeCache`.
- Do not cache workflow execution results.
- Do not cache mutable child workflow runtime state.
- Do not make Redis required for workflow execution.
- Do not change workflow tool create/update/delete semantics.
- Do not use Redis cache as a substitute for short-lived DB sessions.

## Desired Behavior

`WorkflowToolProviderController.from_db_by_id(provider_id, tenant_id)` becomes read-through cached:

```text
1. If cache is enabled, Redis GET metadata cache key.
2. On hit, build WorkflowToolProviderController from cached metadata.
3. On miss, use a short-lived DB session to load provider/app/account/workflow metadata.
4. Build the controller from DB metadata.
5. Store a JSON-safe metadata payload in Redis with TTL.
6. Return the controller.
```

All Redis failures must fail open:

```text
Redis get/set/delete/lock error -> log at warning/debug as appropriate -> continue through DB path
```

Workflow execution must not fail because Redis is unavailable.

## Cache Key

Use a versioned tenant/provider scoped key:

```text
workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:v1
```

If `tenant_id` is not provided, prefer resolving through the DB path rather than using a less-specific cache key. Runtime workflow tool lookup should pass `tenant_id`, so cache coverage remains high for the hot path.

Lock key:

```text
workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:lock
```

## Configuration

Add feature config:

```env
WORKFLOW_TOOL_PROVIDER_CACHE_TTL=300
WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT=3
WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT=0.2
WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL=0.05
```

Semantics:

- `WORKFLOW_TOOL_PROVIDER_CACHE_TTL=300` stores metadata for 5 minutes.
- `WORKFLOW_TOOL_PROVIDER_CACHE_TTL=0` disables the Redis metadata cache.
- Lock timeout bounds the lifetime of the Redis singleflight lock.
- Wait timeout and interval bound how long a request waits for another request to populate the cache before falling back to DB.

## Cache Payload

Cache enough data to build the existing `WorkflowToolProviderController` and `WorkflowTool` without DB access on Redis hit.

Suggested JSON shape:

```json
{
  "schema_version": 1,
  "provider": {
    "id": "...",
    "tenant_id": "...",
    "app_id": "...",
    "user_id": "...",
    "name": "...",
    "label": "...",
    "description": "...",
    "icon": "...",
    "version": "...",
    "parameter_configurations": []
  },
  "app": {
    "id": "...",
    "tenant_id": "...",
    "mode": "workflow",
    "name": "...",
    "icon": "...",
    "icon_background": "...",
    "app_model_config_id": "...",
    "workflow_id": "..."
  },
  "workflow": {
    "id": "...",
    "tenant_id": "...",
    "app_id": "...",
    "version": "...",
    "graph": {},
    "features": {},
    "created_by": "...",
    "created_at": "...",
    "updated_at": "..."
  },
  "user": {
    "id": "...",
    "name": "..."
  }
}
```

The exact field list should be derived from the fields accessed by:

- `WorkflowToolProviderController._get_db_provider_tool(...)`
- `WorkflowTool._resolve_app_and_workflow()`
- `WorkflowTool._invoke(...)`
- `WorkflowAppGenerator.generate(...)`
- tracing/logging paths that consume the detached `App` and `Workflow`

On Redis hit, reconstruct detached `App` and `Workflow` model instances and pass them through the existing `workflow_entities` field:

```python
WorkflowTool(
    workflow_entities={
        "app": app,
        "workflow": workflow,
    },
)
```

Avoid lazy-loading relationships from cached model instances. Cached instances must contain all fields needed by the hot path.

## Singleflight / Stampede Protection

On cache miss, use a lightweight Redis lock:

```text
GET cache
miss:
  acquire lock with timeout=WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT, blocking_timeout=0
    if acquired:
      re-check cache
      if still miss: DB load + SETEX
    if not acquired:
      retry GET every WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL
      stop after WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT
      if still miss: DB fallback without failing workflow
```

This reduces cold-cache stampede while keeping workflow latency bounded. Lock acquisition failure or Redis errors must not block workflow execution beyond the configured wait timeout.

## Invalidation

Invalidate by deleting the provider cache key after successful DB commit.

Primary mutation paths:

```text
api/services/tools/workflow_tools_manage_service.py
  create_workflow_tool(...)
  update_workflow_tool(...)
  delete_workflow_tool(...)
```

Delete cache after commit succeeds:

```text
create/update: commit -> delete cache key
 delete: determine provider id/tenant -> commit -> delete cache key
```

Prefer delete over set after commit. Delete is simpler and avoids writing stale metadata if the in-memory object does not exactly match committed DB state.

Also inspect and cover app deletion cleanup:

```text
api/tasks/remove_app_and_related_data_task.py
```

If workflow tool providers are batch-deleted by app id, load affected provider ids and tenant ids before deletion, then delete their cache keys after commit.

TTL remains the fallback if an invalidation path is missed.

## Error Handling

- Redis GET parse failure: log, delete the bad key if possible, fallback DB.
- Payload schema version mismatch: treat as miss, delete key if possible, fallback DB.
- Redis SETEX failure: return DB-built controller; do not fail execution.
- Redis DELETE failure during invalidation: log warning; rely on TTL for eventual self-healing.
- Lock failure: skip singleflight and fallback DB after bounded retry.

## Security / Privacy

The cache payload includes workflow graph, features, app metadata, provider descriptions, and user display names. Treat it as sensitive tenant data.

Requirements:

- Key must include tenant id.
- Never cache across tenants.
- Do not include secrets or credentials unless the current DB path already requires them for workflow tool execution. The initial design should not cache credentials.
- Respect existing Redis deployment security assumptions.

## Testing Strategy

Unit tests for cache helper / provider controller:

- TTL `0` disables Redis cache and uses DB path.
- Redis hit builds a `WorkflowToolProviderController` without DB access.
- Redis miss loads DB metadata, builds controller, and calls `setex` with configured TTL.
- Redis GET failure falls back to DB.
- Redis SETEX failure returns the DB-built controller.
- Invalid JSON or schema mismatch is treated as miss and does not fail workflow execution.
- Lock acquired path re-checks cache before DB load.
- Lock busy path waits briefly and returns cache if another request populates it.
- Lock busy path falls back to DB after wait timeout.

Unit tests for invalidation:

- `create_workflow_tool()` deletes the cache key after successful commit.
- `update_workflow_tool()` deletes the cache key after successful commit.
- `delete_workflow_tool()` deletes the cache key after successful commit.
- App deletion task invalidates affected workflow tool provider cache keys.
- Failed commit does not delete or set cache based on uncommitted data.

Focused integration/load validation:

- Run nested workflow-as-tool pressure test with Redis cache enabled.
- Confirm DB query count for `tool_workflow_providers`, `apps`, `workflows`, and `accounts` drops after warm cache.
- Confirm Redis miss storm does not produce a DB query spike proportional to concurrency when singleflight is enabled.
- Confirm workflow execution succeeds if Redis is stopped or unavailable.

## Compatibility

The feature is backward-compatible:

- Disabled by setting TTL to `0`.
- Redis failure falls back to existing DB path.
- Existing per-run cache behavior remains unchanged.
- Existing workflow tool mutation semantics remain unchanged.

## Implementation Notes

Suggested structure:

```text
api/core/tools/workflow_as_tool/provider_cache.py
```

Responsibilities:

- Build cache keys.
- Serialize DB-loaded metadata payload.
- Deserialize cached payload.
- Reconstruct detached `App` and `Workflow` instances.
- Wrap Redis operations with fail-open behavior.
- Provide invalidation function:
  ```python
  invalidate_workflow_tool_provider_cache(tenant_id: str, provider_id: str) -> None
  ```

Keep `WorkflowToolProviderController` focused on building provider controllers. It can call the cache helper, but Redis-specific serialization and locking logic should live in a separate module for testability.
