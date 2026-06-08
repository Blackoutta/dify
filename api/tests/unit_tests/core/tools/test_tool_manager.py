from types import SimpleNamespace

import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.tools import tool_manager as tool_manager_module
from core.tools.entities.tool_entities import ToolInvokeFrom, ToolProviderType
from core.tools.errors import ToolProviderNotFoundError
from core.tools.tool_manager import ToolManager
from core.tools.workflow_as_tool import provider_cache
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import WorkflowToolRuntimeCache
from models.tools import WorkflowToolProvider


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

    monkeypatch.setattr(
        tool_manager_module,
        "session_factory",
        SimpleNamespace(create_session=fake_create_session),
        raising=False,
    )
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


def test_get_tool_runtime_workflow_uses_run_level_cache_on_second_call(monkeypatch):
    cache = WorkflowToolRuntimeCache()
    loads = {"count": 0}
    forked_runtime_ids = []

    class FakeWorkflowTool:
        workflow_app_id = "app-1"

        def fork_tool_runtime(self, runtime):
            forked_runtime_ids.append(id(runtime))
            return SimpleNamespace(runtime=runtime, workflow_app_id=self.workflow_app_id, prototype=self)

    class FakeController:
        def __init__(self):
            self.tool = FakeWorkflowTool()

        def get_tools(self, tenant_id):
            assert tenant_id == "tenant-1"
            return [self.tool]

    def fake_from_db_by_id(provider_id, *, tenant_id=None):
        assert provider_id == "provider-1"
        assert tenant_id == "tenant-1"
        loads["count"] += 1
        return FakeController()

    monkeypatch.setattr("core.tools.tool_manager.WorkflowTool", FakeWorkflowTool)
    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.from_db_by_id", fake_from_db_by_id)

    first = ToolManager.get_tool_runtime(
        provider_type=ToolProviderType.WORKFLOW,
        provider_id="provider-1",
        tool_name="child_workflow",
        tenant_id="tenant-1",
        invoke_from=InvokeFrom.SERVICE_API,
        tool_invoke_from=ToolInvokeFrom.WORKFLOW,
        workflow_tool_runtime_cache=cache,
    )
    second = ToolManager.get_tool_runtime(
        provider_type=ToolProviderType.WORKFLOW,
        provider_id="provider-1",
        tool_name="child_workflow",
        tenant_id="tenant-1",
        invoke_from=InvokeFrom.SERVICE_API,
        tool_invoke_from=ToolInvokeFrom.WORKFLOW,
        workflow_tool_runtime_cache=cache,
    )

    assert loads["count"] == 1
    assert first.prototype is second.prototype
    assert first is not second
    assert first.runtime is not second.runtime
    assert len(set(forked_runtime_ids)) == 2


def test_get_tool_runtime_workflow_cache_is_scoped_by_cache_object_and_key(monkeypatch):
    cache1 = WorkflowToolRuntimeCache()
    cache2 = WorkflowToolRuntimeCache()
    loads = {"count": 0}

    class FakeWorkflowTool:
        workflow_app_id = "app-1"

        def __init__(self, label):
            self.label = label

        def fork_tool_runtime(self, runtime):
            return SimpleNamespace(runtime=runtime, label=self.label, workflow_app_id="app-1")

    class FakeController:
        def __init__(self, label):
            self.tool = FakeWorkflowTool(label)

        def get_tools(self, tenant_id):
            return [self.tool]

    def fake_from_db_by_id(provider_id, *, tenant_id=None):
        loads["count"] += 1
        return FakeController(f"{tenant_id}:{provider_id}:{loads['count']}")

    monkeypatch.setattr("core.tools.tool_manager.WorkflowTool", FakeWorkflowTool)
    monkeypatch.setattr("core.tools.tool_manager.WorkflowToolProviderController.from_db_by_id", fake_from_db_by_id)

    a1 = ToolManager.get_tool_runtime(
        ToolProviderType.WORKFLOW,
        "provider-1",
        "child",
        "tenant-1",
        workflow_tool_runtime_cache=cache1,
    )
    a2 = ToolManager.get_tool_runtime(
        ToolProviderType.WORKFLOW,
        "provider-1",
        "child",
        "tenant-1",
        workflow_tool_runtime_cache=cache1,
    )
    b1 = ToolManager.get_tool_runtime(
        ToolProviderType.WORKFLOW,
        "provider-1",
        "child",
        "tenant-1",
        workflow_tool_runtime_cache=cache2,
    )
    c1 = ToolManager.get_tool_runtime(
        ToolProviderType.WORKFLOW,
        "provider-2",
        "child",
        "tenant-1",
        workflow_tool_runtime_cache=cache1,
    )
    d1 = ToolManager.get_tool_runtime(
        ToolProviderType.WORKFLOW,
        "provider-1",
        "child",
        "tenant-2",
        workflow_tool_runtime_cache=cache1,
    )

    assert a1.label == a2.label
    assert b1.label != a1.label
    assert c1.label != a1.label
    assert d1.label != a1.label
    assert loads["count"] == 4


def test_generate_workflow_tool_icon_url_uses_provider_metadata_cache_without_db(monkeypatch):
    metadata = provider_cache.WorkflowToolProviderCacheMetadata(
        provider=WorkflowToolProvider(
            id="provider-1",
            tenant_id="tenant-1",
            app_id="app-1",
            user_id="account-1",
            name="child",
            label="Child",
            description="desc",
            icon='{"background":"#fff","content":"🤖"}',
            version="1",
            parameter_configuration="[]",
        ),
        app=SimpleNamespace(),
        workflow=SimpleNamespace(),
        user=None,
    )
    db_used = {"value": False}

    monkeypatch.setattr(
        "core.tools.tool_manager.provider_cache.get_or_load_workflow_tool_provider_metadata",
        lambda tenant_id, provider_id, loader: metadata,
    )
    monkeypatch.setattr(
        "core.tools.tool_manager.db.session.query",
        lambda *args, **kwargs: db_used.__setitem__("value", True),
    )

    icon = ToolManager.generate_workflow_tool_icon_url("tenant-1", "provider-1")

    assert icon == {"background": "#fff", "content": "🤖"}
    assert db_used["value"] is False


def test_generate_workflow_tool_icon_url_falls_back_to_db_on_cache_miss(monkeypatch):
    workflow_provider = SimpleNamespace(icon='{"background":"#000","content":"🛠️"}')
    queried = {"value": False}

    class FakeQuery:
        def filter(self, *criteria):
            queried["value"] = True
            return self

        def first(self):
            return workflow_provider

    def cache_miss_loader(tenant_id, provider_id, loader):
        return loader()

    monkeypatch.setattr(
        "core.tools.tool_manager.provider_cache.get_or_load_workflow_tool_provider_metadata",
        cache_miss_loader,
    )
    monkeypatch.setattr("core.tools.tool_manager.db.session.query", lambda *args, **kwargs: FakeQuery())

    icon = ToolManager.generate_workflow_tool_icon_url("tenant-1", "provider-1")

    assert icon == {"background": "#000", "content": "🛠️"}
    assert queried["value"] is True


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
