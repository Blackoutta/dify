import json
from datetime import datetime

from configs import dify_config
from core.tools.workflow_as_tool import provider_cache
from models.account import Account
from models.model import App
from models.tools import WorkflowToolProvider
from models.workflow import Workflow


def test_workflow_tool_provider_cache_config_values_are_available():
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_TTL >= 0
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT > 0
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT >= 0
    assert dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL > 0


def test_workflow_tool_provider_cache_key_includes_tenant_provider_and_version():
    assert (
        provider_cache.workflow_tool_provider_cache_key("tenant-1", "provider-1")
        == "workflow_tool_provider:tenant:tenant-1:provider:provider-1:v1"
    )
    assert (
        provider_cache.workflow_tool_provider_lock_key("tenant-1", "provider-1")
        == "workflow_tool_provider:tenant:tenant-1:provider:provider-1:lock"
    )


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
    workflow = Workflow(
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
    )
    workflow._environment_variables = "{}"
    workflow._conversation_variables = "{}"
    return workflow


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

    assert metadata is not None
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
