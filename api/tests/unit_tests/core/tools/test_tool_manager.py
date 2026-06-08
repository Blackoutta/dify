from types import SimpleNamespace

import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.tools.entities.tool_entities import ToolInvokeFrom, ToolProviderType
from core.tools.errors import ToolProviderNotFoundError
from core.tools.tool_manager import ToolManager


def test_get_tool_runtime_workflow_provider_builds_controller_without_outer_lookup_session(monkeypatch):
    global_session_used = {"value": False}
    create_session_used = {"value": False}

    class FakeWorkflowTool:
        def fork_tool_runtime(self, runtime):
            return SimpleNamespace(runtime=runtime, workflow_app_id="app-1")

    class FakeController:
        provider_id = "provider-1"

        def get_tools(self, tenant_id):
            assert tenant_id == "tenant-1"
            return [FakeWorkflowTool()]

    def fake_create_session():
        create_session_used["value"] = True
        raise AssertionError("ToolManager must not open an outer workflow provider lookup session")

    def fake_from_db_by_id(provider_id, *, tenant_id=None):
        assert provider_id == "provider-1"
        assert tenant_id == "tenant-1"
        return FakeController()

    def fail_if_nested_controller_conversion_runs(db_provider):
        raise AssertionError("workflow provider controller conversion must not run inside provider lookup session")

    monkeypatch.setattr("core.tools.tool_manager.session_factory.create_session", fake_create_session)
    monkeypatch.setattr(
        "core.tools.tool_manager.db.session.query",
        lambda *args, **kwargs: global_session_used.__setitem__("value", True),
    )
    monkeypatch.setattr(
        "core.tools.tool_manager.ToolTransformService.workflow_provider_to_controller",
        fail_if_nested_controller_conversion_runs,
    )
    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.from_db_by_id", fake_from_db_by_id)

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
    assert create_session_used["value"] is False
    assert global_session_used["value"] is False


def test_get_tool_runtime_workflow_provider_missing_raises(monkeypatch):
    def fake_from_db_by_id(provider_id, *, tenant_id=None):
        raise ValueError("workflow provider not found")

    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.from_db_by_id", fake_from_db_by_id)

    with pytest.raises(ToolProviderNotFoundError, match="workflow provider provider-1 not found"):
        ToolManager.get_tool_runtime(
            provider_type=ToolProviderType.WORKFLOW,
            provider_id="provider-1",
            tool_name="child_workflow",
            tenant_id="tenant-1",
        )
