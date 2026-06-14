from contextlib import contextmanager

import core.tools.workflow_as_tool.provider as provider_module
from core.tools.entities.common_entities import I18nObject
from core.tools.entities.tool_entities import ToolProviderEntity, ToolProviderIdentity
from core.tools.workflow_as_tool import provider_cache
from core.tools.workflow_as_tool.provider import WorkflowToolProviderController
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
        icon="{}",
        version="1",
        parameter_configuration="[]",
    )


def _controller_without_tools():
    controller = WorkflowToolProviderController(
        entity=ToolProviderEntity(
            identity=ToolProviderIdentity(
                author="",
                name="Child Workflow",
                label=I18nObject(en_US="Child Workflow", zh_Hans="Child Workflow"),
                description=I18nObject(en_US="Child Workflow", zh_Hans="Child Workflow"),
                icon="{}",
            ),
            credentials_schema=[],
            plugin_id=None,
        ),
        provider_id="provider-1",
    )
    controller.tools = None
    return controller


def test_from_db_reloads_provider_with_short_session(monkeypatch):
    provider = _provider_row()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    user = Account(id="account-1", name="Alice", email="alice@example.com")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    closed = {"value": False}
    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            closed["value"] = True

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

        def where(self, *args):
            return self

        def first(self):
            if self.queried_model is WorkflowToolProvider:
                return provider
            if self.queried_model is Workflow:
                return workflow
            return None

    monkeypatch.setattr("core.tools.workflow_as_tool.provider.session_factory.create_session", lambda: FakeSession())

    controller = WorkflowToolProviderController.from_db(provider)

    assert controller.provider_id == "provider-1"
    assert controller.tools[0].workflow_app_id == "app-1"
    assert controller.tools[0].version == "1"
    assert closed["value"] is True
    assert not hasattr(provider_module, "db")


def test_get_tools_uses_provider_id_not_app_id_and_short_session(monkeypatch):
    controller = _controller_without_tools()
    provider = _provider_row()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    where_text = []

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        @contextmanager
        def begin(self):
            yield self

        def query(self, model):
            return self

        def where(self, *criteria):
            where_text.extend(str(item) for item in criteria)
            return self

        def first(self):
            return provider

        def get(self, model, primary_key):
            if model is App:
                return app
            return None

    monkeypatch.setattr("core.tools.workflow_as_tool.provider.session_factory.create_session", lambda: FakeSession())
    monkeypatch.setattr(
        "core.tools.workflow_as_tool.provider.WorkflowToolProviderController._get_db_provider_tool",
        lambda self, db_provider, app, session, user=None: type("FakeTool", (), {"workflow_app_id": app.id})(),
    )

    tools = controller.get_tools("tenant-1")

    assert tools[0].workflow_app_id == "app-1"
    assert any("tool_workflow_providers.id" in item for item in where_text)
    assert not any("tool_workflow_providers.app_id" in item for item in where_text)


def test_from_db_by_id_uses_cached_metadata_without_db(monkeypatch):
    metadata = provider_cache.WorkflowToolProviderCacheMetadata(
        provider=_provider_row(),
        app=App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
        workflow=Workflow(
            id="workflow-1",
            tenant_id="tenant-1",
            app_id="app-1",
            type="workflow",
            version="1",
            graph="{}",
            features="{}",
            created_by="account-1",
        ),
        user=Account(id="account-1", name="Alice", email="alice@example.com"),
    )
    db_used = {"value": False}

    def fail_create_session():
        db_used["value"] = True
        raise AssertionError("cache hit must not open DB session")

    monkeypatch.setattr(
        provider_cache,
        "get_cached_workflow_tool_provider_metadata",
        lambda tenant_id, provider_id: metadata,
    )
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
    workflow = Workflow(
        id="workflow-1",
        tenant_id="tenant-1",
        app_id="app-1",
        type="workflow",
        version="1",
        graph="{}",
        features="{}",
        created_by="account-1",
    )
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

    monkeypatch.setattr(
        provider_cache,
        "get_cached_workflow_tool_provider_metadata",
        lambda tenant_id, provider_id: None,
    )
    monkeypatch.setattr(
        provider_cache,
        "set_cached_workflow_tool_provider_metadata",
        lambda tenant_id, provider_id, payload: set_calls.append((tenant_id, provider_id, payload)),
    )
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

    monkeypatch.setattr(provider_cache, "get_cached_workflow_tool_provider_metadata", fake_get_cached_metadata)
    monkeypatch.setattr(
        "core.tools.workflow_as_tool.provider.WorkflowToolProviderController._load_metadata_from_db",
        lambda provider_id, tenant_id=None: provider_cache.WorkflowToolProviderCacheMetadata(
            provider=_provider_row(),
            app=App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
            workflow=Workflow(
                id="workflow-1",
                tenant_id="tenant-1",
                app_id="app-1",
                type="workflow",
                version="1",
                graph="{}",
                features="{}",
                created_by="account-1",
            ),
            user=None,
        ),
    )

    controller = WorkflowToolProviderController.from_db_by_id("provider-1")

    assert controller.provider_id == "provider-1"
    assert cache_get_used["value"] is False
