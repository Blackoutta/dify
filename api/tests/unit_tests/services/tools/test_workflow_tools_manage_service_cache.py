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
    session.query_results = [
        None,
        SimpleNamespace(id="app-1", tenant_id="tenant-1", workflow=SimpleNamespace(version="1")),
    ]
    invalidated = []

    monkeypatch.setattr("services.tools.workflow_tools_manage_service.db.session", session)
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.WorkflowToolConfigurationUtils.check_parameter_configurations",
        lambda parameters: None,
    )
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.WorkflowToolProviderController.from_db",
        lambda provider: None,
    )
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache",
        lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, session.committed)),
    )

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
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.WorkflowToolConfigurationUtils.check_parameter_configurations",
        lambda parameters: None,
    )
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.WorkflowToolProviderController.from_db",
        lambda provider: None,
    )
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache",
        lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, session.committed)),
    )

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
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache",
        lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, session.committed)),
    )

    result = WorkflowToolManageService.delete_workflow_tool("account-1", "tenant-1", "provider-1")

    assert result == {"result": "success"}
    assert invalidated == [("tenant-1", "provider-1", True)]


def test_create_workflow_tool_failed_commit_does_not_invalidate(monkeypatch):
    class FailingCommitSession(FakeSession):
        def commit(self):
            raise RuntimeError("commit failed")

    session = FailingCommitSession()
    session.query_results = [
        None,
        SimpleNamespace(id="app-1", tenant_id="tenant-1", workflow=SimpleNamespace(version="1")),
    ]
    invalidated = []

    monkeypatch.setattr("services.tools.workflow_tools_manage_service.db.session", session)
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.WorkflowToolConfigurationUtils.check_parameter_configurations",
        lambda parameters: None,
    )
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.WorkflowToolProviderController.from_db",
        lambda provider: None,
    )
    monkeypatch.setattr(
        "services.tools.workflow_tools_manage_service.invalidate_workflow_tool_provider_cache",
        lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id)),
    )

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
