from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from core.ops.entities.config_entity import TracingProviderEnum
from services.trace_config_batch_service import TraceConfigBatchError, TraceConfigBatchService


def test_list_providers_returns_all_supported_provider_values():
    providers = TraceConfigBatchService.list_providers()

    assert providers == sorted([provider.value for provider in TracingProviderEnum])


def test_get_template_marks_required_fields_and_uses_defaults():
    template = TraceConfigBatchService.get_template("langfuse")

    assert template == {
        "provider": "langfuse",
        "app_ids": ["<app-id>"],
        "tracing_config": {
            "public_key": "<required>",
            "secret_key": "<required>",
            "host": "https://api.langfuse.com",
        },
        "enable": True,
    }


def test_get_all_templates_contains_each_supported_provider():
    templates = TraceConfigBatchService.get_all_templates()

    assert set(templates.keys()) == {provider.value for provider in TracingProviderEnum}
    assert templates["aliyun"]["tracing_config"]["license_key"] == "<required>"
    assert templates["aliyun"]["tracing_config"]["endpoint"] == "<required>"
    assert templates["aliyun"]["tracing_config"]["app_name"] == "dify_app"


def test_normalize_provider_rejects_unsupported_provider():
    with pytest.raises(TraceConfigBatchError, match="Unsupported tracing provider"):
        TraceConfigBatchService.normalize_provider("not-real")


def test_validate_tracing_config_rejects_invalid_schema():
    with pytest.raises(TraceConfigBatchError, match="Invalid tracing config"):
        TraceConfigBatchService.validate_tracing_config("langfuse", {"public_key": "pk-only"})


def test_validate_tracing_config_applies_provider_defaults():
    config = TraceConfigBatchService.validate_tracing_config(
        "langfuse",
        {"public_key": "pk", "secret_key": "sk"},
    )

    assert config == {
        "public_key": "pk",
        "secret_key": "sk",
        "host": "https://api.langfuse.com",
    }


def test_validate_credentials_skips_external_check_when_requested():
    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective") as api_check:
        TraceConfigBatchService.validate_credentials(
            "langfuse", {"public_key": "pk", "secret_key": "sk"}, validate=False
        )

    api_check.assert_not_called()


def test_validate_credentials_calls_external_check_once_when_requested():
    with patch(
        "services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective",
        return_value=True,
    ) as api_check:
        TraceConfigBatchService.validate_credentials(
            "langfuse", {"public_key": "pk", "secret_key": "sk"}, validate=True
        )

    api_check.assert_called_once_with(
        {"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        "langfuse",
    )


def test_validate_credentials_raises_when_external_check_fails():
    with patch(
        "services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective",
        return_value=False,
    ):
        with pytest.raises(TraceConfigBatchError, match="Invalid Credentials"):
            TraceConfigBatchService.validate_credentials(
                "langfuse",
                {"public_key": "pk", "secret_key": "sk"},
                validate=True,
            )


@dataclass
class FakeTraceConfig:
    app_id: str
    tracing_provider: str
    tracing_config: dict


class FakeSession:
    def __init__(self):
        self.apps = {}
        self.trace_configs = {}
        self.tenants = []
        self.workspace_apps = {}
        self.added = []
        self.commits = 0
        self.rollbacks = 0

    def add(self, item):
        self.added.append(item)
        self.trace_configs[(item.app_id, item.tracing_provider)] = item

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def install_fake_lookup(monkeypatch, fake_session):
    def fake_load_app(app_id):
        return fake_session.apps.get(app_id)

    def fake_load_config(app_id, provider):
        return fake_session.trace_configs.get((app_id, provider))

    monkeypatch.setattr("services.trace_config_batch_service.db.session", fake_session)
    monkeypatch.setattr(TraceConfigBatchService, "_load_app", staticmethod(fake_load_app))
    monkeypatch.setattr(TraceConfigBatchService, "_load_trace_config", staticmethod(fake_load_config))
    monkeypatch.setattr("services.trace_config_batch_service.TraceAppConfig", FakeTraceConfig)


class FakeListQuery:
    def __init__(self, items):
        self.items = items

    def filter(self, *args):
        return self

    def order_by(self, *args):
        return self

    def all(self):
        return self.items


def test_list_workspaces_returns_workspace_options(monkeypatch):
    fake_session = FakeSession()
    fake_session.tenants = [
        SimpleNamespace(id="tenant-1", name="Workspace One"),
        SimpleNamespace(id="tenant-2", name=None),
    ]

    def fake_query(model):
        assert model.__name__ == "Tenant"
        return FakeListQuery(fake_session.tenants)

    monkeypatch.setattr("services.trace_config_batch_service.db.session.query", fake_query)

    assert TraceConfigBatchService.list_workspaces() == [
        {"id": "tenant-1", "name": "Workspace One"},
        {"id": "tenant-2", "name": "tenant-2"},
    ]


def test_list_apps_for_workspace_returns_app_options(monkeypatch):
    fake_session = FakeSession()
    fake_session.workspace_apps["tenant-1"] = [
        SimpleNamespace(id="app-1", name="App One", mode="chat"),
        SimpleNamespace(id="app-2", name=None, mode="workflow"),
    ]

    class WorkspaceAppQuery(FakeListQuery):
        def filter(self, *args):
            return FakeListQuery(fake_session.workspace_apps["tenant-1"])

    def fake_query(model):
        assert model.__name__ == "App"
        return WorkspaceAppQuery([])

    monkeypatch.setattr("services.trace_config_batch_service.db.session.query", fake_query)

    assert TraceConfigBatchService.list_apps_for_workspace("tenant-1") == [
        {"id": "app-1", "name": "App One", "mode": "chat"},
        {"id": "app-2", "name": "app-2", "mode": "workflow"},
    ]


def test_batch_upsert_creates_config_when_none_exists(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with (
        patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True),
        patch(
            "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
            return_value={
                "public_key": "encrypted-pk",
                "secret_key": "encrypted-sk",
                "host": "https://api.langfuse.com",
            },
        ) as encrypt_config,
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    encrypt_config.assert_called_once_with(
        "tenant-1",
        "langfuse",
        {"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        None,
    )
    assert result.succeeded == 1
    assert result.failed == 0
    assert result.results[0].status == "created"
    assert fake_session.trace_configs[("app-1", "langfuse")].tracing_config["public_key"] == "encrypted-pk"
    assert fake_session.commits == 1


def test_batch_upsert_updates_config_when_one_exists(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    fake_session.trace_configs[("app-1", "langfuse")] = FakeTraceConfig(
        app_id="app-1",
        tracing_provider="langfuse",
        tracing_config={"public_key": "old", "secret_key": "old", "host": "https://api.langfuse.com"},
    )
    install_fake_lookup(monkeypatch, fake_session)

    with (
        patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True),
        patch(
            "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
            return_value={"public_key": "new", "secret_key": "new", "host": "https://api.langfuse.com"},
        ) as encrypt_config,
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    encrypt_config.assert_called_once_with(
        "tenant-1",
        "langfuse",
        {"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        {"public_key": "old", "secret_key": "old", "host": "https://api.langfuse.com"},
    )
    assert result.results[0].status == "updated"
    assert fake_session.trace_configs[("app-1", "langfuse")].tracing_config["public_key"] == "new"
    assert fake_session.commits == 1


def test_batch_upsert_enables_app_tracing_by_default(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with (
        patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True),
        patch(
            "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
            return_value={
                "public_key": "encrypted-pk",
                "secret_key": "encrypted-sk",
                "host": "https://api.langfuse.com",
            },
        ),
        patch("services.trace_config_batch_service.OpsTraceManager.update_app_tracing_config") as update_tracing,
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
        )

    update_tracing.assert_called_once_with("app-1", True, "langfuse")
    assert result.results[0].enabled is True
    assert result.results[0].status == "created, enabled"


def test_batch_upsert_does_not_enable_when_enable_false(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with (
        patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True),
        patch(
            "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
            return_value={
                "public_key": "encrypted-pk",
                "secret_key": "encrypted-sk",
                "host": "https://api.langfuse.com",
            },
        ),
        patch("services.trace_config_batch_service.OpsTraceManager.update_app_tracing_config") as update_tracing,
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    update_tracing.assert_not_called()
    assert result.results[0].enabled is False
    assert result.results[0].status == "created"


def test_batch_upsert_continues_after_per_app_failure_by_default(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-2"] = SimpleNamespace(id="app-2", tenant_id="tenant-1", name="App Two", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with (
        patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True),
        patch(
            "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
            return_value={
                "public_key": "encrypted-pk",
                "secret_key": "encrypted-sk",
                "host": "https://api.langfuse.com",
            },
        ),
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["missing-app", "app-2"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    assert result.total == 2
    assert result.failed == 1
    assert result.succeeded == 1
    assert result.results[0].status == "failed"
    assert result.results[0].error == "App not found"
    assert result.results[1].status == "created"
    assert fake_session.rollbacks == 1


def test_batch_upsert_stops_after_first_per_app_failure_with_fail_fast(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-2"] = SimpleNamespace(id="app-2", tenant_id="tenant-1", name="App Two", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch(
        "services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["missing-app", "app-2"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
            fail_fast=True,
        )

    assert result.total == 1
    assert result.failed == 1
    assert result.results[0].app_id == "missing-app"


def test_batch_level_schema_failure_happens_before_app_writes(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with pytest.raises(TraceConfigBatchError, match="Invalid tracing config"):
        TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk-only"},
        )

    assert fake_session.commits == 0
    assert fake_session.added == []


def test_external_validation_is_called_once_for_multiple_apps(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    fake_session.apps["app-2"] = SimpleNamespace(id="app-2", tenant_id="tenant-1", name="App Two", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with (
        patch(
            "services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True
        ) as api_check,
        patch(
            "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
            return_value={
                "public_key": "encrypted-pk",
                "secret_key": "encrypted-sk",
                "host": "https://api.langfuse.com",
            },
        ),
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1", "app-2"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    assert result.succeeded == 2
    api_check.assert_called_once()
