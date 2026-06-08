from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.tools.entities.tool_entities import ToolInvokeFrom, ToolProviderType
from core.tools.errors import ToolProviderNotFoundError
from core.tools.tool_manager import ToolManager
from models.tools import WorkflowToolProvider


def _workflow_provider():
    return WorkflowToolProvider(
        id="provider-1",
        tenant_id="tenant-1",
        app_id="app-1",
        name="child_workflow",
        label="Child Workflow",
        description="Child workflow as tool",
        icon="{}",
        version="1",
        parameter_configuration="[]",
    )


def test_get_tool_runtime_workflow_provider_lookup_uses_short_session(monkeypatch):
    provider = _workflow_provider()
    closed = {"value": False}
    global_session_used = {"value": False}

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            closed["value"] = True

        @contextmanager
        def begin(self):
            yield self

        def query(self, model):
            assert model is WorkflowToolProvider
            return self

        def filter(self, *criteria):
            return self

        def first(self):
            return provider

    class FakeWorkflowTool:
        def fork_tool_runtime(self, runtime):
            return SimpleNamespace(runtime=runtime, workflow_app_id="app-1")

    get_tools_called_after_session_closed = {"value": False}

    def fake_get_tools(self, tenant_id):
        assert tenant_id == "tenant-1"
        assert self.provider_id == "provider-1"
        get_tools_called_after_session_closed["value"] = closed["value"]
        return [FakeWorkflowTool()]

    def fail_if_nested_controller_conversion_runs(db_provider):
        raise AssertionError("workflow provider controller conversion must not run inside provider lookup session")

    monkeypatch.setattr("core.tools.tool_manager.session_factory.create_session", lambda: FakeSession())
    monkeypatch.setattr(
        "core.tools.tool_manager.db.session.query",
        lambda *args, **kwargs: global_session_used.__setitem__("value", True),
    )
    monkeypatch.setattr(
        "core.tools.tool_manager.ToolTransformService.workflow_provider_to_controller",
        fail_if_nested_controller_conversion_runs,
    )
    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.get_tools", fake_get_tools)

    runtime = ToolManager.get_tool_runtime(
        provider_type=ToolProviderType.WORKFLOW,
        provider_id="provider-1",
        tool_name="child_workflow",
        tenant_id="tenant-1",
        invoke_from=InvokeFrom.SERVICE_API,
        tool_invoke_from=ToolInvokeFrom.WORKFLOW,
    )

    assert runtime.workflow_app_id == "app-1"
    assert runtime.runtime.tenant_id == "tenant-1"
    assert closed["value"] is True
    assert get_tools_called_after_session_closed["value"] is True
    assert global_session_used["value"] is False


def test_get_tool_runtime_workflow_provider_missing_raises(monkeypatch):
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

        def filter(self, *criteria):
            return self

        def first(self):
            return None

    monkeypatch.setattr("core.tools.tool_manager.session_factory.create_session", lambda: FakeSession())

    with pytest.raises(ToolProviderNotFoundError, match="workflow provider provider-1 not found"):
        ToolManager.get_tool_runtime(
            provider_type=ToolProviderType.WORKFLOW,
            provider_id="provider-1",
            tool_name="child_workflow",
            tenant_id="tenant-1",
        )
