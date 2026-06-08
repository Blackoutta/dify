# Workflow Tool Provider Redis Cache Development Diary

Date: 2026-06-08

## Scope

This diary covers the Redis metadata cache feature for workflow-as-tool provider resolution. The squash range is `574a22e53a..99b08a5771`, which starts with the design spec and ends with the final query-estimate documentation update.

## References

- Design spec: `docs/superpowers/specs/2026-06-08-workflow-tool-provider-redis-cache-design.md`
- Implementation plan: `docs/superpowers/plans/2026-06-08-workflow-tool-provider-redis-cache.md`
- DB call estimate note: `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md`

## Problem

The earlier per-run `WorkflowToolRuntimeCache` removes repeated metadata loads inside one parent workflow run, but it does not help when many concurrent parent runs first-call the same workflow tool. Under a `-c 100` pressure test, each parent run can independently resolve the same provider/app/account/workflow metadata and produce a cross-request first-call stampede.

## Design Decisions

### Read-through Redis metadata cache

`WorkflowToolProviderController.from_db_by_id(provider_id, tenant_id=...)` now resolves provider metadata through a Redis read-through helper when a tenant id is available. Redis hits reconstruct detached `WorkflowToolProvider`, `App`, `Workflow`, and optional `Account` metadata, then build the same controller/tool shape as the DB path.

The cache key is tenant/provider scoped and versioned:

```text
workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:v1
```

If no tenant id is provided, the controller uses the DB path rather than a less-specific cache key.

### Fail-open Redis behavior

Redis errors must not break workflow execution. GET, SETEX, DELETE, and lock failures log warnings and fall back to the existing DB path or TTL-based eventual consistency.

### Bounded singleflight

Cold misses use a short Redis lock:

```text
workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:lock
```

The lock holder rechecks cache, loads DB metadata, and populates Redis. Lock losers wait briefly for cache population and then fall back to DB if the wait budget expires. This bounds cold-cache stampedes without making Redis required.

### Commit-after invalidation

Workflow tool create/update/delete paths delete the Redis key only after successful DB commit. App deletion invalidates affected provider keys after each delete commit. Delete-after-commit avoids dirty cache writes after rollback.

### Workflow response icon path

After implementation, load-test SQL sampling still showed `tool_workflow_providers` lookups during `/v1/workflows/run`. Source tracing showed these were not console list calls and not workflow runtime metadata calls. `workflow_response_converter.py` adds `extras["icon"]` for every tool node event, which calls `ToolManager.get_tool_icon()` and then `generate_workflow_tool_icon_url()`. That method had a direct `WorkflowToolProvider` DB query and bypassed the provider metadata cache. The fix routes workflow tool icon generation through the same provider metadata cache with direct DB fallback for compatibility.

## Implementation Summary

- Added workflow cache configuration to `WorkflowConfig`:
  - `WORKFLOW_TOOL_PROVIDER_CACHE_TTL`
  - `WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT`
  - `WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT`
  - `WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL`
- Added `api/core/tools/workflow_as_tool/provider_cache.py` for:
  - key construction
  - payload serialization/deserialization
  - detached model reconstruction
  - fail-open Redis operations
  - singleflight get-or-load behavior
  - invalidation
- Refactored `WorkflowToolProviderController` to build controllers from metadata and to use the Redis helper for tenant-scoped lookups.
- Added invalidation to workflow tool management service create/update/delete commits.
- Added invalidation to app deletion cleanup of `tool_workflow_providers`.
- Updated workflow tool icon generation to reuse the provider metadata cache before falling back to the previous direct DB lookup.
- Updated DB call documentation to distinguish Redis cold miss, Redis warm hit, same-run cache hit, and response icon behavior.

## Verification Performed

Focused verification:

```bash
cd api && uv run pytest -o addopts='' \
  tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py \
  tests/unit_tests/core/tools/workflow_as_tool/test_provider.py \
  tests/unit_tests/core/tools/workflow_as_tool/test_tool.py \
  tests/unit_tests/core/tools/test_tool_manager.py \
  tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py \
  tests/unit_tests/tasks/test_remove_app_and_related_data_task.py \
  -v
```

Result before icon follow-up: `40 passed`.

After the icon-path follow-up, focused workflow/tool verification was rerun:

```bash
cd api && uv run pytest -o addopts='' \
  tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py \
  tests/unit_tests/core/tools/workflow_as_tool/test_provider.py \
  tests/unit_tests/core/tools/workflow_as_tool/test_tool.py \
  tests/unit_tests/core/tools/test_tool_manager.py \
  tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py \
  tests/unit_tests/tasks/test_remove_app_and_related_data_task.py \
  -v
```

Expected result after follow-up: all tests pass.

Broader tools/service verification:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools tests/unit_tests/services/tools -v
```

Result: `41 passed`.

Import smoke:

```bash
cd api && uv run python - <<'PY'
from core.tools.workflow_as_tool.provider import WorkflowToolProviderController
from core.tools.workflow_as_tool.provider_cache import (
    get_or_load_workflow_tool_provider_metadata,
    invalidate_workflow_tool_provider_cache,
    workflow_tool_provider_cache_key,
)
print("ok")
PY
```

Result: `ok`.

## Commit Appendix

| Commit | Subject | Notes |
| --- | --- | --- |
| `3be5cdc72c` | docs: design workflow tool provider redis cache | Product design spec for cross-request Redis metadata cache. |
| `2fa39e8b05` | docs: plan workflow tool provider redis cache | Detailed TDD implementation plan. |
| `f46d49906c` | feat: add workflow tool provider cache config | Added config defaults and cache/lock key helpers. |
| `28fdeb0f93` | feat: serialize workflow tool provider cache metadata | Added payload serialization and detached model reconstruction. |
| `3c3450fc81` | feat: add fail-open workflow tool provider redis cache helpers | Added fail-open get/set/delete Redis helpers. |
| `40e912a9ed` | feat: use redis cache for workflow tool provider metadata | Integrated cache with `WorkflowToolProviderController`. |
| `ad25777ab5` | feat: add workflow tool provider cache singleflight | Added lock-based bounded cold-miss singleflight. |
| `7e9b336cc8` | feat: invalidate workflow tool provider cache on mutations | Added create/update/delete invalidation in management service. |
| `33364abb56` | feat: invalidate workflow tool provider cache on app deletion | Added app deletion cleanup invalidation. |
| `99b08a5771` | docs: update workflow tool redis cache query estimates | Updated DB round-trip estimates for Redis cold/warm behavior. |
