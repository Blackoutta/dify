from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.entities.workflow_tool_runtime_cache import (
    WorkflowToolRuntimeCache,
    WorkflowToolRuntimeCacheKey,
)


def _state() -> GraphRuntimeState:
    return GraphRuntimeState(variable_pool=VariablePool(system_variables={}, user_inputs={}), start_at=0)


def test_graph_runtime_state_gets_independent_workflow_tool_runtime_cache():
    first = _state()
    second = _state()

    assert isinstance(first.workflow_tool_runtime_cache, WorkflowToolRuntimeCache)
    assert isinstance(second.workflow_tool_runtime_cache, WorkflowToolRuntimeCache)
    assert first.workflow_tool_runtime_cache is not second.workflow_tool_runtime_cache

    key = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-1", tool_name="child")
    first.workflow_tool_runtime_cache.workflow_tools[key] = object()

    assert key in first.workflow_tool_runtime_cache.workflow_tools
    assert key not in second.workflow_tool_runtime_cache.workflow_tools


def test_workflow_tool_runtime_cache_key_separates_tenant_provider_and_tool():
    cache = WorkflowToolRuntimeCache()
    key = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-1", tool_name="child")
    other_tenant = WorkflowToolRuntimeCacheKey(tenant_id="tenant-2", provider_id="provider-1", tool_name="child")
    other_provider = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-2", tool_name="child")
    other_tool = WorkflowToolRuntimeCacheKey(tenant_id="tenant-1", provider_id="provider-1", tool_name="other")

    cached_tool = object()
    cache.workflow_tools[key] = cached_tool

    assert cache.workflow_tools[key] is cached_tool
    assert other_tenant not in cache.workflow_tools
    assert other_provider not in cache.workflow_tools
    assert other_tool not in cache.workflow_tools
