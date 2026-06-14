# Workflow Tool Provider Redis Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a fail-open Redis read-through cache with bounded singleflight protection for workflow-as-tool provider metadata across parent workflow runs.

**Architecture:** Keep the existing per-run `WorkflowToolRuntimeCache` unchanged. Add a focused `provider_cache.py` helper that owns Redis keys, serialization, deserialization, detached model reconstruction, invalidation, and singleflight behavior. `WorkflowToolProviderController.from_db_by_id()` delegates cache orchestration to that helper while preserving short-lived DB sessions and current controller/tool construction semantics.

**Tech Stack:** Python 3.12, SQLAlchemy models, Pydantic settings, Redis via `extensions.ext_redis.redis_client`, pytest, monkeypatch-based unit tests.

---

## File Structure

- Create `api/core/tools/workflow_as_tool/provider_cache.py`
  - Builds cache and lock keys.
  - Reads Redis configuration from `configs.dify_config`.
  - Serializes DB-loaded `WorkflowToolProvider`, `App`, `Account`, and `Workflow` metadata into JSON-safe payloads.
  - Deserializes payloads and reconstructs detached SQLAlchemy model instances.
  - Wraps Redis get/set/delete/lock operations in fail-open behavior.
  - Provides invalidation API used by management services and app deletion task.

- Modify `api/configs/feature/__init__.py`
  - Add four workflow tool provider cache settings to `WorkflowConfig`.

- Modify `api/core/tools/workflow_as_tool/provider.py`
  - Extract DB-load path into a private classmethod.
  - Call the cache helper from `from_db_by_id(provider_id, tenant_id=...)` only when `tenant_id` is provided and TTL is positive.
  - Add a controller builder that can build from already-loaded metadata without querying DB.

- Modify `api/services/tools/workflow_tools_manage_service.py`
  - Delete provider cache after successful create/update/delete commits.

- Modify `api/tasks/remove_app_and_related_data_task.py`
  - Invalidate workflow tool provider cache keys after each app-deletion workflow tool provider commit.

- Create `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`
  - Unit tests for cache keys, serialization, Redis hits/misses, fail-open behavior, malformed payloads, and lock behavior.

- Modify `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py`
  - Tests that provider controller uses Redis hit without DB and falls back to DB when tenant id is absent or cache disabled.

- Create `api/tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py`
  - Unit tests for create/update/delete invalidation after successful commit and no invalidation before failed commit.

- Create or modify `api/tests/unit_tests/tasks/test_remove_app_and_related_data_task.py`
  - Unit test for app deletion workflow tool provider invalidation.

---

### Task 1: Add workflow tool provider cache configuration

**Files:**
- Modify: `api/configs/feature/__init__.py`
- Test: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`

- [ ] **Step 1: Write the failing config test**

Create `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py` with this initial content:

```python
from configs import dify_config
from core.tools.workflow_as_tool import provider_cache


def test_workflow_tool_provider_cache_config_defaults_are_available():
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_TTL == 300
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT == 3
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT == 0.2
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL == 0.05


def test_workflow_tool_provider_cache_key_includes_tenant_provider_and_version():
    assert (
        provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")
        == "workflow_tool_provider:tenant:tenant-1:provider:provider-1:v1"
    )
    assert (
        provider_cache.workflow_tool_provider_lock_key("tenant-1", "provider-1")
        == "workflow_tool_provider:tenant:tenant-1:provider:provider-1:lock"
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: FAIL because `provider_cache` does not exist or config fields do not exist.

- [ ] **Step 3: Add config fields**

In `api/configs/feature/__init__.py`, add these fields inside `class WorkflowConfig(BaseSettings):` after `WORKFLOW_CALL_MAX_DEPTH`:

```python
    WORKFLOW_TOOL_PROVIDER_CACHE_TTL: NonNegativeInt = Field(
        description="Redis TTL in seconds for workflow tool provider metadata cache. Set to 0 to disable.",
        default=300,
    )

    WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT: PositiveFloat = Field(
        description="Redis lock lifetime in seconds for workflow tool provider metadata cache singleflight.",
        default=3.0,
    )

    WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT: NonNegativeFloat = Field(
        description="Maximum seconds to wait for another request to populate workflow tool provider metadata cache.",
        default=0.2,
    )

    WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL: PositiveFloat = Field(
        description="Seconds between Redis cache rechecks while waiting for workflow tool provider metadata cache fill.",
        default=0.05,
    )
```

Create `api/core/tools/workflow_as_tool/provider_cache.py` with the key helpers:

```python
from __future__ import annotations

CACHE_SCHEMA_VERSION = 1


def workflow_tool_provider_cache_key(tenant_id: str, provider_id: str) -> str:
    return f"workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:v1"


def workflow_tool_provider_lock_key(tenant_id: str, provider_id: str) -> str:
    return f"workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:lock"
```

- [ ] **Step 4: Run test to verify it passes**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: PASS with 2 tests passing.

- [ ] **Step 5: Commit**

```bash
git add api/configs/feature/__init__.py api/core/tools/workflow_as_tool/provider_cache.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py
git commit -m "feat: add workflow tool provider cache config"
```

---

### Task 2: Add metadata payload serialization and detached model reconstruction

**Files:**
- Modify: `api/core/tools/workflow_as_tool/provider_cache.py`
- Modify: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`

- [ ] **Step 1: Add failing serialization tests**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`:

```python
from datetime import datetime

from models.account import Account
from models.model import App
from models.tools import WorkflowToolProvider
from models.workflow import Workflow


def _provider_row():
    return WorkflowToolProvider(
        id="provider-1",
        tenant_id="tenant-1",
        app_id="app-1",
        user_id="account-1",
        name="child_workflow",
        label="Child Workflow",
        description="Child workflow as tool",
        icon='{"emoji":"🤖"}',
        version="1",
        parameter_configuration='[{"name":"query","description":"Query","form":"llm"}]',
    )


def _app_row():
    return App(
        id="app-1",
        tenant_id="tenant-1",
        mode="workflow",
        name="Child App",
        description="Child app description",
        icon_type="emoji",
        icon="🤖",
        icon_background="#fff",
        app_model_config_id=None,
        workflow_id="workflow-1",
        enable_site=False,
        enable_api=True,
        api_rpm=0,
        api_rph=0,
        is_demo=False,
        is_public=False,
        is_universal=False,
        tracing=None,
        max_active_requests=None,
        created_by="account-1",
        updated_by="account-1",
        use_icon_as_answer_icon=False,
    )


def _workflow_row():
    return Workflow(
        id="workflow-1",
        tenant_id="tenant-1",
        app_id="app-1",
        type="workflow",
        version="1",
        graph='{"nodes":[],"edges":[]}',
        features='{}',
        created_by="account-1",
        updated_by="account-1",
        created_at=datetime(2026, 1, 1),
        updated_at=datetime(2026, 1, 2),
        environment_variables="[]",
        conversation_variables="[]",
    )


def test_build_payload_and_reconstruct_detached_models():
    payload = provider_cache.build_workflow_tool_provider_cache_payload(
        provider=_provider_row(),
        app=_app_row(),
        workflow=_workflow_row(),
        user=Account(id="account-1", name="Alice", email="alice@example.com"),
    )

    assert payload["schema_version"] == 1
    assert payload["provider"]["id"] == "provider-1"
    assert payload["provider"]["parameter_configuration"] == '[{"name":"query","description":"Query","form":"llm"}]'
    assert payload["app"]["id"] == "app-1"
    assert payload["workflow"]["graph"] == '{"nodes":[],"edges":[]}'
    assert payload["user"] == {"id": "account-1", "name": "Alice"}

    metadata = provider_cache.models_from_workflow_tool_provider_cache_payload(payload)

    assert metadata.provider.id == "provider-1"
    assert metadata.provider.parameter_configurations[0].name == "query"
    assert metadata.app.id == "app-1"
    assert metadata.app.workflow_id == "workflow-1"
    assert metadata.workflow.id == "workflow-1"
    assert metadata.workflow.graph_dict == {"nodes": [], "edges": []}
    assert metadata.workflow.features_dict == {}
    assert metadata.user is not None
    assert metadata.user.name == "Alice"


def test_models_from_payload_rejects_wrong_schema_version():
    payload = provider_cache.build_workflow_tool_provider_cache_payload(
        provider=_provider_row(),
        app=_app_row(),
        workflow=_workflow_row(),
        user=None,
    )
    payload["schema_version"] = 999

    assert provider_cache.models_from_workflow_tool_provider_cache_payload(payload) is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: FAIL because payload helpers are not implemented.

- [ ] **Step 3: Implement payload helpers**

Replace `api/core/tools/workflow_as_tool/provider_cache.py` with:

```python
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from models.account import Account
from models.model import App
from models.tools import WorkflowToolProvider
from models.workflow import Workflow

CACHE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class WorkflowToolProviderCacheMetadata:
    provider: WorkflowToolProvider
    app: App
    workflow: Workflow
    user: Account | None


def workflow_tool_provider_cache_key(tenant_id: str, provider_id: str) -> str:
    return f"workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:v1"


def workflow_tool_provider_lock_key(tenant_id: str, provider_id: str) -> str:
    return f"workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:lock"


def _datetime_to_payload(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _datetime_from_payload(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def build_workflow_tool_provider_cache_payload(
    *,
    provider: WorkflowToolProvider,
    app: App,
    workflow: Workflow,
    user: Account | None,
) -> dict[str, Any]:
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "provider": {
            "id": provider.id,
            "tenant_id": provider.tenant_id,
            "app_id": provider.app_id,
            "user_id": provider.user_id,
            "name": provider.name,
            "label": provider.label,
            "description": provider.description,
            "icon": provider.icon,
            "version": provider.version,
            "parameter_configuration": provider.parameter_configuration,
            "privacy_policy": provider.privacy_policy,
        },
        "app": {
            "id": app.id,
            "tenant_id": app.tenant_id,
            "name": app.name,
            "description": app.description,
            "mode": app.mode,
            "icon_type": app.icon_type,
            "icon": app.icon,
            "icon_background": app.icon_background,
            "app_model_config_id": app.app_model_config_id,
            "workflow_id": app.workflow_id,
            "status": app.status,
            "enable_site": app.enable_site,
            "enable_api": app.enable_api,
            "api_rpm": app.api_rpm,
            "api_rph": app.api_rph,
            "is_demo": app.is_demo,
            "is_public": app.is_public,
            "is_universal": app.is_universal,
            "tracing": app.tracing,
            "max_active_requests": app.max_active_requests,
            "created_by": app.created_by,
            "updated_by": app.updated_by,
            "use_icon_as_answer_icon": app.use_icon_as_answer_icon,
        },
        "workflow": {
            "id": workflow.id,
            "tenant_id": workflow.tenant_id,
            "app_id": workflow.app_id,
            "type": workflow.type,
            "version": workflow.version,
            "marked_name": workflow.marked_name,
            "marked_comment": workflow.marked_comment,
            "graph": workflow.graph,
            "features": workflow.features,
            "created_by": workflow.created_by,
            "created_at": _datetime_to_payload(workflow.created_at),
            "updated_by": workflow.updated_by,
            "updated_at": _datetime_to_payload(workflow.updated_at),
            "environment_variables": workflow.environment_variables,
            "conversation_variables": workflow.conversation_variables,
        },
        "user": None if user is None else {"id": user.id, "name": user.name},
    }


def models_from_workflow_tool_provider_cache_payload(
    payload: dict[str, Any],
) -> WorkflowToolProviderCacheMetadata | None:
    if payload.get("schema_version") != CACHE_SCHEMA_VERSION:
        return None

    provider_payload = payload["provider"]
    app_payload = payload["app"]
    workflow_payload = payload["workflow"]
    user_payload = payload.get("user")

    provider = WorkflowToolProvider(
        id=provider_payload["id"],
        tenant_id=provider_payload["tenant_id"],
        app_id=provider_payload["app_id"],
        user_id=provider_payload["user_id"],
        name=provider_payload["name"],
        label=provider_payload["label"],
        description=provider_payload["description"],
        icon=provider_payload["icon"],
        version=provider_payload["version"],
        parameter_configuration=provider_payload["parameter_configuration"],
        privacy_policy=provider_payload.get("privacy_policy") or "",
    )

    app = App(**app_payload)

    workflow = Workflow(
        id=workflow_payload["id"],
        tenant_id=workflow_payload["tenant_id"],
        app_id=workflow_payload["app_id"],
        type=workflow_payload["type"],
        version=workflow_payload["version"],
        marked_name=workflow_payload.get("marked_name") or "",
        marked_comment=workflow_payload.get("marked_comment") or "",
        graph=workflow_payload["graph"],
        features=workflow_payload["features"],
        created_by=workflow_payload["created_by"],
        created_at=_datetime_from_payload(workflow_payload.get("created_at")),
        updated_by=workflow_payload.get("updated_by"),
        updated_at=_datetime_from_payload(workflow_payload.get("updated_at")),
        environment_variables=workflow_payload.get("environment_variables") or "[]",
        conversation_variables=workflow_payload.get("conversation_variables") or "[]",
    )

    user = None if user_payload is None else Account(id=user_payload["id"], name=user_payload["name"])
    return WorkflowToolProviderCacheMetadata(provider=provider, app=app, workflow=workflow, user=user)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: PASS with 4 tests passing.

- [ ] **Step 5: Commit**

```bash
git add api/core/tools/workflow_as_tool/provider_cache.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py
git commit -m "feat: serialize workflow tool provider cache metadata"
```

---

### Task 3: Implement fail-open Redis get/set/delete behavior

**Files:**
- Modify: `api/core/tools/workflow_as_tool/provider_cache.py`
- Modify: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`

- [ ] **Step 1: Add failing Redis operation tests**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`:

```python
import json


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.setex_calls = []
        self.deleted = []

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.setex_calls.append((key, ttl, value))
        self.values[key] = value.encode("utf-8") if isinstance(value, str) else value

    def delete(self, key):
        self.deleted.append(key)
        self.values.pop(key, None)


def test_get_cached_metadata_returns_none_when_ttl_disabled(monkeypatch):
    redis = FakeRedis()
    redis.values[provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")] = b"{}"
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 0)

    assert provider_cache.get_cached_workflow_tool_provider_metadata("tenant-1", "provider-1") is None


def test_get_cached_metadata_decodes_payload(monkeypatch):
    redis = FakeRedis()
    payload = provider_cache.build_workflow_tool_provider_cache_payload(
        provider=_provider_row(),
        app=_app_row(),
        workflow=_workflow_row(),
        user=None,
    )
    redis.values[provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")] = json.dumps(
        payload
    ).encode("utf-8")
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)

    metadata = provider_cache.get_cached_workflow_tool_provider_metadata("tenant-1", "provider-1")

    assert metadata is not None
    assert metadata.provider.id == "provider-1"
    assert metadata.workflow.id == "workflow-1"


def test_get_cached_metadata_deletes_bad_json_and_returns_none(monkeypatch):
    redis = FakeRedis()
    key = provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")
    redis.values[key] = b"not-json"
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)

    assert provider_cache.get_cached_workflow_tool_provider_metadata("tenant-1", "provider-1") is None
    assert redis.deleted == [key]


def test_set_cached_metadata_uses_ttl(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)
    payload = provider_cache.build_workflow_tool_provider_cache_payload(
        provider=_provider_row(),
        app=_app_row(),
        workflow=_workflow_row(),
        user=None,
    )

    provider_cache.set_cached_workflow_tool_provider_metadata("tenant-1", "provider-1", payload)

    key, ttl, value = redis.setex_calls[0]
    assert key == provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")
    assert ttl == 300
    assert json.loads(value)["provider"]["id"] == "provider-1"


def test_redis_get_and_set_fail_open(monkeypatch):
    class FailingRedis(FakeRedis):
        def get(self, key):
            raise RuntimeError("redis down")

        def setex(self, key, ttl, value):
            raise RuntimeError("redis down")

    monkeypatch.setattr(provider_cache, "redis_client", FailingRedis())
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)

    assert provider_cache.get_cached_workflow_tool_provider_metadata("tenant-1", "provider-1") is None
    provider_cache.set_cached_workflow_tool_provider_metadata("tenant-1", "provider-1", {"schema_version": 1})


def test_invalidate_workflow_tool_provider_cache_deletes_key_and_fails_open(monkeypatch):
    redis = FakeRedis()
    monkeypatch.setattr(provider_cache, "redis_client", redis)

    provider_cache.invalidate_workflow_tool_provider_cache("tenant-1", "provider-1")

    assert redis.deleted == [provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")]

    class FailingDeleteRedis(FakeRedis):
        def delete(self, key):
            raise RuntimeError("redis down")

    monkeypatch.setattr(provider_cache, "redis_client", FailingDeleteRedis())
    provider_cache.invalidate_workflow_tool_provider_cache("tenant-1", "provider-1")
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: FAIL because Redis helpers do not exist.

- [ ] **Step 3: Implement Redis helpers**

In `api/core/tools/workflow_as_tool/provider_cache.py`, add imports:

```python
import json
import logging
from json import JSONDecodeError

from configs import dify_config
from extensions.ext_redis import redis_client
```

Add these functions below `models_from_workflow_tool_provider_cache_payload`:

```python
logger = logging.getLogger(__name__)


def _cache_enabled() -> bool:
    return dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_TTL > 0


def _decode_redis_value(value: bytes | str) -> dict[str, Any]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("workflow tool provider cache payload must be a JSON object")
    return decoded


def get_cached_workflow_tool_provider_metadata(
    tenant_id: str,
    provider_id: str,
) -> WorkflowToolProviderCacheMetadata | None:
    if not _cache_enabled():
        return None

    key = workflow_tool_provider_cache_key(tenant_id, provider_id)
    try:
        cached_value = redis_client.get(key)
    except Exception:
        logger.warning("Failed to get workflow tool provider metadata cache", exc_info=True)
        return None

    if not cached_value:
        return None

    try:
        payload = _decode_redis_value(cached_value)
        metadata = models_from_workflow_tool_provider_cache_payload(payload)
    except (JSONDecodeError, UnicodeDecodeError, KeyError, TypeError, ValueError):
        logger.warning("Invalid workflow tool provider metadata cache payload", exc_info=True)
        invalidate_workflow_tool_provider_cache(tenant_id, provider_id)
        return None

    if metadata is None:
        invalidate_workflow_tool_provider_cache(tenant_id, provider_id)
        return None

    return metadata


def set_cached_workflow_tool_provider_metadata(
    tenant_id: str,
    provider_id: str,
    payload: dict[str, Any],
) -> None:
    if not _cache_enabled():
        return

    key = workflow_tool_provider_cache_key(tenant_id, provider_id)
    try:
        redis_client.setex(key, dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_TTL, json.dumps(payload))
    except Exception:
        logger.warning("Failed to set workflow tool provider metadata cache", exc_info=True)


def invalidate_workflow_tool_provider_cache(tenant_id: str, provider_id: str) -> None:
    key = workflow_tool_provider_cache_key(tenant_id, provider_id)
    try:
        redis_client.delete(key)
    except Exception:
        logger.warning("Failed to delete workflow tool provider metadata cache", exc_info=True)
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: PASS with all provider cache tests passing.

- [ ] **Step 5: Commit**

```bash
git add api/core/tools/workflow_as_tool/provider_cache.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py
git commit -m "feat: add fail-open workflow tool provider redis cache helpers"
```

---

### Task 4: Integrate cache hit and DB miss path into provider controller

**Files:**
- Modify: `api/core/tools/workflow_as_tool/provider.py`
- Modify: `api/core/tools/workflow_as_tool/provider_cache.py`
- Modify: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py`

- [ ] **Step 1: Add failing provider controller tests**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py`:

```python
from core.tools.workflow_as_tool import provider_cache


def test_from_db_by_id_uses_cached_metadata_without_db(monkeypatch):
    metadata = provider_cache.WorkflowToolProviderCacheMetadata(
        provider=_provider_row(),
        app=App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
        workflow=Workflow(id="workflow-1", tenant_id="tenant-1", app_id="app-1", type="workflow", version="1", graph="{}", features="{}", created_by="account-1"),
        user=Account(id="account-1", name="Alice", email="alice@example.com"),
    )
    db_used = {"value": False}

    def fail_create_session():
        db_used["value"] = True
        raise AssertionError("cache hit must not open DB session")

    monkeypatch.setattr(provider_cache, "get_cached_workflow_tool_provider_metadata", lambda tenant_id, provider_id: metadata)
    monkeypatch.setattr(provider_cache, "set_cached_workflow_tool_provider_metadata", lambda *args, **kwargs: None)
    monkeypatch.setattr("core.tools.workflow_as_tool.provider.session_factory.create_session", fail_create_session)

    controller = WorkflowToolProviderController.from_db_by_id("provider-1", tenant_id="tenant-1")

    assert controller.provider_id == "provider-1"
    assert controller.entity.identity.author == "Alice"
    assert controller.tools[0].workflow_app_id == "app-1"
    assert controller.tools[0].workflow_entities["app"].id == "app-1"
    assert controller.tools[0].workflow_entities["workflow"].id == "workflow-1"
    assert db_used["value"] is False


def test_from_db_by_id_cache_miss_loads_db_and_sets_cache(monkeypatch):
    provider = _provider_row()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    user = Account(id="account-1", name="Alice", email="alice@example.com")
    workflow = Workflow(id="workflow-1", tenant_id="tenant-1", app_id="app-1", type="workflow", version="1", graph="{}", features="{}", created_by="account-1")
    set_calls = []

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        @contextmanager
        def begin(self):
            yield self

        def get(self, model, primary_key):
            if model is App:
                return app
            if model is Account:
                return user
            return None

        def query(self, model):
            self.queried_model = model
            return self

        def where(self, *criteria):
            return self

        def first(self):
            if self.queried_model is WorkflowToolProvider:
                return provider
            if self.queried_model is Workflow:
                return workflow
            return None

    monkeypatch.setattr(provider_cache, "get_cached_workflow_tool_provider_metadata", lambda tenant_id, provider_id: None)
    monkeypatch.setattr(provider_cache, "set_cached_workflow_tool_provider_metadata", lambda tenant_id, provider_id, payload: set_calls.append((tenant_id, provider_id, payload)))
    monkeypatch.setattr("core.tools.workflow_as_tool.provider.session_factory.create_session", lambda: FakeSession())

    controller = WorkflowToolProviderController.from_db_by_id("provider-1", tenant_id="tenant-1")

    assert controller.provider_id == "provider-1"
    assert controller.tools[0].workflow_app_id == "app-1"
    assert set_calls[0][0] == "tenant-1"
    assert set_calls[0][1] == "provider-1"
    assert set_calls[0][2]["provider"]["id"] == "provider-1"


def test_from_db_by_id_without_tenant_skips_cache(monkeypatch):
    cache_get_used = {"value": False}

    def fake_get_cached_metadata(tenant_id, provider_id):
        cache_get_used["value"] = True
        return None

    monkeypatch.setattr(provider_cache, "get_cached_workflow_tool_provider_metadata", fake_get_cached_metadata)
    monkeypatch.setattr(
        "core.tools.workflow_as_tool.provider.WorkflowToolProviderController._load_metadata_from_db",
        lambda provider_id, tenant_id=None: provider_cache.WorkflowToolProviderCacheMetadata(
            provider=_provider_row(),
            app=App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
            workflow=Workflow(id="workflow-1", tenant_id="tenant-1", app_id="app-1", type="workflow", version="1", graph="{}", features="{}", created_by="account-1"),
            user=None,
        ),
    )

    controller = WorkflowToolProviderController.from_db_by_id("provider-1")

    assert controller.provider_id == "provider-1"
    assert cache_get_used["value"] is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider.py -v
```

Expected: FAIL because provider controller does not use the cache helper and private methods do not exist.

- [ ] **Step 3: Refactor provider controller**

In `api/core/tools/workflow_as_tool/provider.py`, add import:

```python
from core.tools.workflow_as_tool import provider_cache
```

Replace `from_db_by_id` with:

```python
    @classmethod
    def from_db_by_id(
        cls, provider_id: str, *, tenant_id: str | None = None
    ) -> "WorkflowToolProviderController":
        if tenant_id is not None:
            cached_metadata = provider_cache.get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
            if cached_metadata is not None:
                return cls._from_metadata(cached_metadata)

        metadata = cls._load_metadata_from_db(provider_id, tenant_id=tenant_id)
        if tenant_id is not None:
            payload = provider_cache.build_workflow_tool_provider_cache_payload(
                provider=metadata.provider,
                app=metadata.app,
                workflow=metadata.workflow,
                user=metadata.user,
            )
            provider_cache.set_cached_workflow_tool_provider_metadata(tenant_id, provider_id, payload)

        return cls._from_metadata(metadata)
```

Add these classmethods below `from_db_by_id`:

```python
    @classmethod
    def _load_metadata_from_db(
        cls, provider_id: str, *, tenant_id: str | None = None
    ) -> provider_cache.WorkflowToolProviderCacheMetadata:
        with session_factory.create_session() as session, session.begin():
            provider_query = session.query(WorkflowToolProvider).where(WorkflowToolProvider.id == provider_id)
            if tenant_id is not None:
                provider_query = provider_query.where(WorkflowToolProvider.tenant_id == tenant_id)
            provider = provider_query.first()
            if not provider:
                raise ValueError("workflow provider not found")

            app = session.get(App, provider.app_id)
            if not app:
                raise ValueError("app not found")

            user = session.get(Account, provider.user_id) if provider.user_id else None
            workflow = cls._get_db_provider_workflow(provider, session=session)

            return provider_cache.WorkflowToolProviderCacheMetadata(
                provider=provider,
                app=app,
                workflow=workflow,
                user=user,
            )

    @classmethod
    def _from_metadata(
        cls,
        metadata: provider_cache.WorkflowToolProviderCacheMetadata,
    ) -> "WorkflowToolProviderController":
        controller = WorkflowToolProviderController(
            entity=ToolProviderEntity(
                identity=ToolProviderIdentity(
                    author=metadata.user.name if metadata.user else "",
                    name=metadata.provider.label,
                    label=I18nObject(en_US=metadata.provider.label, zh_Hans=metadata.provider.label),
                    description=I18nObject(en_US=metadata.provider.description, zh_Hans=metadata.provider.description),
                    icon=metadata.provider.icon,
                ),
                credentials_schema=[],
                plugin_id=None,
            ),
            provider_id=metadata.provider.id or "",
        )
        controller.tools = [controller._get_db_provider_tool_from_metadata(metadata)]
        return controller

    @staticmethod
    def _get_db_provider_workflow(db_provider: WorkflowToolProvider, *, session: Session) -> Workflow:
        workflow: Workflow | None = (
            session.query(Workflow)
            .where(Workflow.app_id == db_provider.app_id, Workflow.version == db_provider.version)
            .first()
        )
        if not workflow:
            raise ValueError("workflow not found")
        return workflow
```

Add this instance method before `_get_db_provider_tool`:

```python
    def _get_db_provider_tool_from_metadata(
        self,
        metadata: provider_cache.WorkflowToolProviderCacheMetadata,
    ) -> WorkflowTool:
        return self._build_workflow_tool(
            db_provider=metadata.provider,
            app=metadata.app,
            workflow=metadata.workflow,
            user=metadata.user,
        )
```

Refactor `_get_db_provider_tool` to fetch workflow and delegate:

```python
    def _get_db_provider_tool(
        self,
        db_provider: WorkflowToolProvider,
        app: App,
        *,
        session: Session,
        user: Account | None = None,
    ) -> WorkflowTool:
        workflow = self._get_db_provider_workflow(db_provider, session=session)
        return self._build_workflow_tool(db_provider=db_provider, app=app, workflow=workflow, user=user)
```

Add `_build_workflow_tool` by moving the original body after workflow lookup into this method:

```python
    def _build_workflow_tool(
        self,
        *,
        db_provider: WorkflowToolProvider,
        app: App,
        workflow: Workflow,
        user: Account | None = None,
    ) -> WorkflowTool:
        graph: Mapping = workflow.graph_dict
        features_dict: Mapping = workflow.features_dict
        features = WorkflowAppConfigManager.convert_features(config_dict=features_dict, app_mode=AppMode.WORKFLOW)
        parameters = db_provider.parameter_configurations
        variables = WorkflowToolConfigurationUtils.get_workflow_graph_variables(graph)

        def fetch_workflow_variable(variable_name: str) -> VariableEntity | None:
            return next(filter(lambda x: x.variable == variable_name, variables), None)  # type: ignore

        workflow_tool_parameters = []
        for parameter in parameters:
            variable = fetch_workflow_variable(parameter.name)
            if variable:
                parameter_type = None
                options = []
                if variable.type not in VARIABLE_TO_PARAMETER_TYPE_MAPPING:
                    raise ValueError(f"unsupported variable type {variable.type}")
                parameter_type = VARIABLE_TO_PARAMETER_TYPE_MAPPING[variable.type]
                if variable.type == VariableEntityType.SELECT and variable.options:
                    options = [
                        PluginParameterOption(value=option, label=I18nObject(en_US=option, zh_Hans=option))
                        for option in variable.options
                    ]
                workflow_tool_parameters.append(
                    ToolParameter(
                        name=parameter.name,
                        label=I18nObject(en_US=variable.label, zh_Hans=variable.label),
                        human_description=I18nObject(en_US=parameter.description, zh_Hans=parameter.description),
                        type=parameter_type,
                        form=parameter.form,
                        llm_description=parameter.description,
                        required=variable.required,
                        options=options,
                        placeholder=I18nObject(en_US="", zh_Hans=""),
                    )
                )
            elif features.file_upload:
                workflow_tool_parameters.append(
                    ToolParameter(
                        name=parameter.name,
                        label=I18nObject(en_US=parameter.name, zh_Hans=parameter.name),
                        human_description=I18nObject(en_US=parameter.description, zh_Hans=parameter.description),
                        type=ToolParameter.ToolParameterType.SYSTEM_FILES,
                        llm_description=parameter.description,
                        required=False,
                        form=parameter.form,
                        placeholder=I18nObject(en_US="", zh_Hans=""),
                    )
                )
            else:
                raise ValueError("variable not found")

        return WorkflowTool(
            workflow_as_tool_id=db_provider.id,
            entity=ToolEntity(
                identity=ToolIdentity(
                    author=user.name if user else "",
                    name=db_provider.name,
                    label=I18nObject(en_US=db_provider.label, zh_Hans=db_provider.label),
                    provider=self.provider_id,
                    icon=db_provider.icon,
                ),
                description=ToolDescription(
                    human=I18nObject(en_US=db_provider.description, zh_Hans=db_provider.description),
                    llm=db_provider.description,
                ),
                parameters=workflow_tool_parameters,
            ),
            runtime=ToolRuntime(tenant_id=db_provider.tenant_id),
            workflow_app_id=app.id,
            workflow_entities={"app": app, "workflow": workflow},
            version=db_provider.version,
            workflow_call_depth=0,
            label=db_provider.label,
        )
```

- [ ] **Step 4: Run focused provider tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider.py tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: PASS for provider and provider cache tests.

- [ ] **Step 5: Commit**

```bash
git add api/core/tools/workflow_as_tool/provider.py api/core/tools/workflow_as_tool/provider_cache.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py
git commit -m "feat: use redis cache for workflow tool provider metadata"
```

---

### Task 5: Add bounded Redis singleflight on cache miss

**Files:**
- Modify: `api/core/tools/workflow_as_tool/provider_cache.py`
- Modify: `api/core/tools/workflow_as_tool/provider.py`
- Modify: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`
- Modify: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py`

- [ ] **Step 1: Add failing singleflight tests**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py`:

```python
class FakeLock:
    def __init__(self, acquired=True):
        self.acquired = acquired
        self.released = False

    def acquire(self, blocking=False):
        return self.acquired

    def release(self):
        self.released = True


class LockingRedis(FakeRedis):
    def __init__(self, acquired=True):
        super().__init__()
        self.fake_lock = FakeLock(acquired=acquired)
        self.lock_calls = []

    def lock(self, name, timeout):
        self.lock_calls.append((name, timeout))
        return self.fake_lock


def test_with_workflow_tool_provider_cache_singleflight_lock_acquired_runs_loader_and_sets_cache(monkeypatch):
    redis = LockingRedis(acquired=True)
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT", 3)
    loads = {"count": 0}

    def loader():
        loads["count"] += 1
        return provider_cache.WorkflowToolProviderCacheMetadata(
            provider=_provider_row(),
            app=_app_row(),
            workflow=_workflow_row(),
            user=None,
        )

    metadata = provider_cache.get_or_load_workflow_tool_provider_metadata("tenant-1", "provider-1", loader)

    assert metadata.provider.id == "provider-1"
    assert loads["count"] == 1
    assert redis.lock_calls == [(provider_cache.workflow_tool_provider_lock_key("tenant-1", "provider-1"), 3)]
    assert redis.setex_calls
    assert redis.fake_lock.released is True


def test_singleflight_rechecks_cache_after_lock_acquired(monkeypatch):
    redis = LockingRedis(acquired=True)
    payload = provider_cache.build_workflow_tool_provider_cache_payload(
        provider=_provider_row(), app=_app_row(), workflow=_workflow_row(), user=None
    )
    key = provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")
    calls = {"get": 0}

    def get_with_second_hit(cache_key):
        calls["get"] += 1
        if calls["get"] == 1:
            return None
        return json.dumps(payload).encode("utf-8")

    redis.get = get_with_second_hit
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)

    def loader():
        raise AssertionError("loader must not run after second cache hit")

    metadata = provider_cache.get_or_load_workflow_tool_provider_metadata("tenant-1", "provider-1", loader)

    assert metadata.provider.id == "provider-1"
    assert key.endswith(":v1")


def test_singleflight_lock_busy_waits_for_cache_then_returns_hit(monkeypatch):
    redis = LockingRedis(acquired=False)
    payload = provider_cache.build_workflow_tool_provider_cache_payload(
        provider=_provider_row(), app=_app_row(), workflow=_workflow_row(), user=None
    )
    calls = {"get": 0, "sleep": 0}

    def get_with_later_hit(cache_key):
        calls["get"] += 1
        if calls["get"] < 3:
            return None
        return json.dumps(payload).encode("utf-8")

    redis.get = get_with_later_hit
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.time, "sleep", lambda seconds: calls.__setitem__("sleep", calls["sleep"] + 1))
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT", 0.2)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL", 0.05)

    def loader():
        raise AssertionError("loader must not run when another request populates cache")

    metadata = provider_cache.get_or_load_workflow_tool_provider_metadata("tenant-1", "provider-1", loader)

    assert metadata.provider.id == "provider-1"
    assert calls["sleep"] >= 1


def test_singleflight_lock_busy_falls_back_to_loader_after_wait_timeout(monkeypatch):
    redis = LockingRedis(acquired=False)
    monkeypatch.setattr(provider_cache, "redis_client", redis)
    monkeypatch.setattr(provider_cache.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_TTL", 300)
    monkeypatch.setattr(provider_cache.dify_config, "WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT", 0)
    loads = {"count": 0}

    def loader():
        loads["count"] += 1
        return provider_cache.WorkflowToolProviderCacheMetadata(
            provider=_provider_row(), app=_app_row(), workflow=_workflow_row(), user=None
        )

    metadata = provider_cache.get_or_load_workflow_tool_provider_metadata("tenant-1", "provider-1", loader)

    assert metadata.provider.id == "provider-1"
    assert loads["count"] == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py -v
```

Expected: FAIL because `get_or_load_workflow_tool_provider_metadata` does not exist.

- [ ] **Step 3: Implement singleflight helper**

In `api/core/tools/workflow_as_tool/provider_cache.py`, add imports:

```python
import time
from collections.abc import Callable
```

Add these functions:

```python
def _payload_from_metadata(metadata: WorkflowToolProviderCacheMetadata) -> dict[str, Any]:
    return build_workflow_tool_provider_cache_payload(
        provider=metadata.provider,
        app=metadata.app,
        workflow=metadata.workflow,
        user=metadata.user,
    )


def _try_acquire_lock(tenant_id: str, provider_id: str):
    lock_name = workflow_tool_provider_lock_key(tenant_id, provider_id)
    try:
        lock = redis_client.lock(lock_name, timeout=dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT)
        if lock.acquire(blocking=False):
            return lock
    except Exception:
        logger.warning("Failed to acquire workflow tool provider metadata cache lock", exc_info=True)
    return None


def _release_lock(lock) -> None:
    try:
        lock.release()
    except Exception:
        logger.warning("Failed to release workflow tool provider metadata cache lock", exc_info=True)


def _wait_for_cached_metadata(tenant_id: str, provider_id: str) -> WorkflowToolProviderCacheMetadata | None:
    deadline = time.monotonic() + dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT
    while time.monotonic() < deadline:
        time.sleep(dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL)
        metadata = get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
        if metadata is not None:
            return metadata
    return None


def get_or_load_workflow_tool_provider_metadata(
    tenant_id: str,
    provider_id: str,
    loader: Callable[[], WorkflowToolProviderCacheMetadata],
) -> WorkflowToolProviderCacheMetadata:
    if not _cache_enabled():
        return loader()

    cached_metadata = get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
    if cached_metadata is not None:
        return cached_metadata

    lock = _try_acquire_lock(tenant_id, provider_id)
    if lock is not None:
        try:
            cached_metadata = get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
            if cached_metadata is not None:
                return cached_metadata
            metadata = loader()
            set_cached_workflow_tool_provider_metadata(tenant_id, provider_id, _payload_from_metadata(metadata))
            return metadata
        finally:
            _release_lock(lock)

    waited_metadata = _wait_for_cached_metadata(tenant_id, provider_id)
    if waited_metadata is not None:
        return waited_metadata

    metadata = loader()
    set_cached_workflow_tool_provider_metadata(tenant_id, provider_id, _payload_from_metadata(metadata))
    return metadata
```

- [ ] **Step 4: Update provider controller to use singleflight**

In `api/core/tools/workflow_as_tool/provider.py`, replace `from_db_by_id` with:

```python
    @classmethod
    def from_db_by_id(
        cls, provider_id: str, *, tenant_id: str | None = None
    ) -> "WorkflowToolProviderController":
        if tenant_id is None:
            return cls._from_metadata(cls._load_metadata_from_db(provider_id, tenant_id=None))

        metadata = provider_cache.get_or_load_workflow_tool_provider_metadata(
            tenant_id,
            provider_id,
            lambda: cls._load_metadata_from_db(provider_id, tenant_id=tenant_id),
        )
        return cls._from_metadata(metadata)
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py tests/unit_tests/core/tools/workflow_as_tool/test_provider.py tests/unit_tests/core/tools/test_tool_manager.py -v
```

Expected: PASS for provider cache, provider, and tool manager tests.

- [ ] **Step 6: Commit**

```bash
git add api/core/tools/workflow_as_tool/provider_cache.py api/core/tools/workflow_as_tool/provider.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider_cache.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py
git commit -m "feat: add workflow tool provider cache singleflight"
```

---

### Task 6: Invalidate cache after workflow tool create/update/delete commits

**Files:**
- Modify: `api/services/tools/workflow_tools_manage_service.py`
- Create: `api/tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py`

- [ ] **Step 1: Add failing invalidation tests**

Create `api/tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py`:

```python
from types import SimpleNamespace

import pytest

from services.tools.workflow_tools_manage_service import WorkflowToolManageService


class FakeSession:
    def __init__(self):
        self.committed = False
        self.added = []
        self.delete_count = 0
        self.query_results = []

    def query(self, model):
        return self

    def filter(self, *criteria):
        return self

    def first(self):
        if self.query_results:
            return self.query_results.pop(0)
        return None

    def add(self, value):
        self.added.append(value)
        if getattr(value, "id", None) is None:
            value.id = "provider-1"

    def delete(self):
        self.delete_count += 1

    def commit(self):
        self.committed = True


def test_create_workflow_tool_invalidates_after_commit(monkeypatch):
    session = FakeSession()
    session.query_results = [None, SimpleNamespace(id="app-1", tenant_id="tenant-1", workflow=SimpleNamespace(version="1"))]
    invalidated = []

    monkeypatch.setattr("services.tools.workflow_tools_manage_service.db.session", session)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.WorkflowToolConfigurationUtils.check_parameter_configurations", lambda parameters: None)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.WorkflowToolProviderController.from_db", lambda provider: None)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache", lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, session.committed)))

    result = WorkflowToolManageService.create_workflow_tool(
        user_id="account-1",
        tenant_id="tenant-1",
        workflow_app_id="app-1",
        name="child_workflow",
        label="Child Workflow",
        icon={},
        description="desc",
        parameters=[],
    )

    assert result == {"result": "success"}
    assert invalidated == [("tenant-1", "provider-1", True)]


def test_update_workflow_tool_invalidates_after_commit(monkeypatch):
    provider = SimpleNamespace(
        id="provider-1",
        tenant_id="tenant-1",
        app_id="app-1",
        name="old",
        label="old",
        icon="{}",
        description="old",
        parameter_configuration="[]",
        privacy_policy="",
        version="1",
        updated_at=None,
    )
    app = SimpleNamespace(id="app-1", tenant_id="tenant-1", workflow=SimpleNamespace(version="2"))
    session = FakeSession()
    session.query_results = [None, provider, app]
    invalidated = []

    monkeypatch.setattr("services.tools.workflow_tools_manage_service.db.session", session)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.WorkflowToolConfigurationUtils.check_parameter_configurations", lambda parameters: None)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.WorkflowToolProviderController.from_db", lambda provider: None)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache", lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, session.committed)))

    result = WorkflowToolManageService.update_workflow_tool(
        user_id="account-1",
        tenant_id="tenant-1",
        workflow_tool_id="provider-1",
        name="child_workflow",
        label="Child Workflow",
        icon={},
        description="desc",
        parameters=[],
    )

    assert result == {"result": "success"}
    assert invalidated == [("tenant-1", "provider-1", True)]


def test_delete_workflow_tool_invalidates_after_commit(monkeypatch):
    session = FakeSession()
    invalidated = []

    monkeypatch.setattr("services.tools.workflow_tools_manage_service.db.session", session)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache", lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, session.committed)))

    result = WorkflowToolManageService.delete_workflow_tool("account-1", "tenant-1", "provider-1")

    assert result == {"result": "success"}
    assert invalidated == [("tenant-1", "provider-1", True)]


def test_create_workflow_tool_failed_commit_does_not_invalidate(monkeypatch):
    class FailingCommitSession(FakeSession):
        def commit(self):
            raise RuntimeError("commit failed")

    session = FailingCommitSession()
    session.query_results = [None, SimpleNamespace(id="app-1", tenant_id="tenant-1", workflow=SimpleNamespace(version="1"))]
    invalidated = []

    monkeypatch.setattr("services.tools.workflow_tools_manage_service.db.session", session)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.WorkflowToolConfigurationUtils.check_parameter_configurations", lambda parameters: None)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.WorkflowToolProviderController.from_db", lambda provider: None)
    monkeypatch.setattr("services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache", lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id)))

    with pytest.raises(RuntimeError):
        WorkflowToolManageService.create_workflow_tool(
            user_id="account-1",
            tenant_id="tenant-1",
            workflow_app_id="app-1",
            name="child_workflow",
            label="Child Workflow",
            icon={},
            description="desc",
            parameters=[],
        )

    assert invalidated == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py -v
```

Expected: FAIL because `invalidate_workflow_tool_provider_cache` is not imported or called.

- [ ] **Step 3: Implement invalidation after commits**

In `api/services/tools/workflow_tools_manage_service.py`, add import:

```python
from core.tools.workflow_as_tool.provider_cache import invalidate_workflow_tool_provider_cache
```

After `db.session.commit()` in `create_workflow_tool`, add:

```python
        invalidate_workflow_tool_provider_cache(tenant_id, workflow_tool_provider.id)
```

After `db.session.commit()` in `update_workflow_tool`, add:

```python
        invalidate_workflow_tool_provider_cache(tenant_id, workflow_tool_provider.id)
```

Replace `delete_workflow_tool` body with:

```python
        db.session.query(WorkflowToolProvider).filter(
            WorkflowToolProvider.tenant_id == tenant_id, WorkflowToolProvider.id == workflow_tool_id
        ).delete()

        db.session.commit()
        invalidate_workflow_tool_provider_cache(tenant_id, workflow_tool_id)

        return {"result": "success"}
```

- [ ] **Step 4: Run invalidation tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py -v
```

Expected: PASS with 4 tests passing.

- [ ] **Step 5: Run workflow tool provider tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/services/tools/workflow_tools_manage_service.py api/tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py
git commit -m "feat: invalidate workflow tool provider cache on mutations"
```

---

### Task 7: Invalidate cache during app deletion workflow tool provider cleanup

**Files:**
- Modify: `api/tasks/remove_app_and_related_data_task.py`
- Create or modify: `api/tests/unit_tests/tasks/test_remove_app_and_related_data_task.py`

- [ ] **Step 1: Add failing app deletion invalidation test**

If `api/tests/unit_tests/tasks/test_remove_app_and_related_data_task.py` does not exist, create it. Add this test:

```python
from types import SimpleNamespace

from tasks import remove_app_and_related_data_task as task_module


def test_delete_workflow_tool_providers_invalidates_cache_after_each_commit(monkeypatch):
    invalidated = []
    deleted_ids = []
    commit_count = {"value": 0}

    class FakeQuery:
        def filter(self, *criteria):
            return self

        def delete(self, synchronize_session=False):
            deleted_ids.append("provider-1")

    class FakeSession:
        def query(self, model):
            return FakeQuery()

        def commit(self):
            commit_count["value"] += 1

    class FakeResult:
        rowcount = 1

        def __iter__(self):
            return iter([SimpleNamespace(id="provider-1")])

        def close(self):
            return None

    class EmptyResult:
        rowcount = 0

        def __iter__(self):
            return iter([])

        def close(self):
            return None

    class FakeConnection:
        def __init__(self):
            self.calls = 0

        def execute(self, statement, params):
            self.calls += 1
            if self.calls == 1:
                return FakeResult()
            return EmptyResult()

    class FakeBegin:
        connection = FakeConnection()

        def __enter__(self):
            return self.connection

        def __exit__(self, exc_type, exc, tb):
            return None

    class FakeEngine:
        def begin(self):
            return FakeBegin()

    fake_db = SimpleNamespace(
        session=FakeSession(),
        engine=FakeEngine(),
        text=lambda value: value,
    )

    monkeypatch.setattr(task_module, "db", fake_db)
    monkeypatch.setattr(task_module, "invalidate_workflow_tool_provider_cache", lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, commit_count["value"])))

    task_module._delete_workflow_tool_providers("tenant-1", "app-1")

    assert deleted_ids == ["provider-1"]
    assert invalidated == [("tenant-1", "provider-1", 1)]
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/tasks/test_remove_app_and_related_data_task.py::test_delete_workflow_tool_providers_invalidates_cache_after_each_commit -v
```

Expected: FAIL because app deletion task does not import or call invalidation.

- [ ] **Step 3: Implement app deletion invalidation**

In `api/tasks/remove_app_and_related_data_task.py`, add import:

```python
from core.tools.workflow_as_tool.provider_cache import invalidate_workflow_tool_provider_cache
```

In `_delete_workflow_tool_providers`, change `del_tool_provider` to:

```python
    def del_tool_provider(tool_provider_id: str):
        db.session.query(WorkflowToolProvider).filter(WorkflowToolProvider.id == tool_provider_id).delete(
            synchronize_session=False
        )
```

Then update `_delete_records` to accept an optional `after_commit` callback:

```python
def _delete_records(
    query_sql: str,
    params: dict,
    delete_func: Callable,
    name: str,
    after_commit: Callable[[str], None] | None = None,
) -> None:
    while True:
        with db.engine.begin() as conn:
            rs = conn.execute(db.text(query_sql), params)
            if rs.rowcount == 0:
                break

            for i in rs:
                record_id = str(i.id)
                try:
                    delete_func(record_id)
                    db.session.commit()
                    if after_commit is not None:
                        after_commit(record_id)
                    logging.info(click.style(f"Deleted {name} {record_id}", fg="green"))
                except Exception:
                    logging.exception(f"Error occurred while deleting {name} {record_id}")
                    continue
            rs.close()
```

Update `_delete_workflow_tool_providers` call to `_delete_records`:

```python
    _delete_records(
        """select id from tool_workflow_providers where tenant_id=:tenant_id and app_id=:app_id limit 1000""",
        {"tenant_id": tenant_id, "app_id": app_id},
        del_tool_provider,
        "tool workflow provider",
        after_commit=lambda tool_provider_id: invalidate_workflow_tool_provider_cache(tenant_id, tool_provider_id),
    )
```

Existing `_delete_records(...)` callers continue to work because `after_commit` has a default value.

- [ ] **Step 4: Run app deletion test**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/tasks/test_remove_app_and_related_data_task.py::test_delete_workflow_tool_providers_invalidates_cache_after_each_commit -v
```

Expected: PASS.

- [ ] **Step 5: Run broader task/service tests touched by deletion helper**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/tasks/test_remove_app_and_related_data_task.py tests/unit_tests/services/tools/test_workflow_tools_manage_service_cache.py -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add api/tasks/remove_app_and_related_data_task.py api/tests/unit_tests/tasks/test_remove_app_and_related_data_task.py
git commit -m "feat: invalidate workflow tool provider cache on app deletion"
```

---

### Task 8: Final verification, docs note, and import smoke

**Files:**
- Modify: `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md` if the metadata estimate still describes only per-run cache behavior.
- No production code changes unless verification reveals a concrete failure.

- [ ] **Step 1: Run focused test suite**

Run:

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

Expected: PASS.

- [ ] **Step 2: Run broader tools tests**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools tests/unit_tests/services/tools -v
```

Expected: PASS. If unrelated service tests require fixtures unavailable in this worktree, run the focused command from Step 1 and record the unavailable fixture error in the final report.

- [ ] **Step 3: Run import smoke**

Run:

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

Expected: `ok` and exit code 0.

- [ ] **Step 4: Update DB call estimate doc if needed**

If `docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md` still says cross-run first calls always perform provider/app/account/workflow SELECTs, update the metadata section to distinguish:

```text
Redis cold miss: ~4 metadata SELECTs, bounded by singleflight under concurrency.
Redis warm hit: ~0 provider/app/account/workflow metadata SELECTs for provider resolution.
Per-run hit: ~0 additional metadata SELECTs inside the same parent workflow run.
```

- [ ] **Step 5: Run formatting/lint through pre-commit by committing final docs if changed**

If Step 4 changed docs, commit:

```bash
git add docs/superpowers/specs/2026-06-07-nested-workflow-sync-db-calls.md
git commit -m "docs: update workflow tool redis cache query estimates"
```

If Step 4 did not change docs, do not create an empty commit.

- [ ] **Step 6: Final status check**

Run:

```bash
git status --short
git log --oneline -8
```

Expected: clean worktree and recent commits for each completed task.

---

## Self-Review Notes

- Spec coverage:
  - Read-through cache: Tasks 3, 4, and 5.
  - Cache key and lock key: Task 1.
  - TTL/configuration: Task 1 and Task 3.
  - Complete metadata payload: Task 2.
  - Detached `App` and `Workflow`: Task 2 and Task 4.
  - Redis singleflight/stampede protection: Task 5.
  - Create/update/delete invalidation: Task 6.
  - App deletion invalidation: Task 7.
  - Fail-open Redis behavior: Task 3 and Task 5.
  - Safety/tenant scoping: Tasks 1, 3, and 4 use tenant-scoped keys and skip cache without tenant id.
  - Testing and verification: Tasks 1 through 8.

- Placeholder scan: This plan has concrete file paths, code snippets, commands, expected outcomes, and commit messages for every task.

- Type consistency: `WorkflowToolProviderCacheMetadata`, `build_workflow_tool_provider_cache_payload`, `models_from_workflow_tool_provider_cache_payload`, `get_cached_workflow_tool_provider_metadata`, `set_cached_workflow_tool_provider_metadata`, `get_or_load_workflow_tool_provider_metadata`, and `invalidate_workflow_tool_provider_cache` are introduced before use by later tasks.
