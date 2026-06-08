from contextlib import contextmanager

import core.tools.workflow_as_tool.provider as provider_module
from core.tools.entities.common_entities import I18nObject
from core.tools.entities.tool_entities import ToolProviderEntity, ToolProviderIdentity
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
