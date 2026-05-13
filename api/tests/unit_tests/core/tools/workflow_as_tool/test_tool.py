import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.ops.trace_context import ParentTraceContext
from core.tools.__base.tool_runtime import ToolRuntime
from core.tools.entities.common_entities import I18nObject
from core.tools.entities.tool_entities import ToolEntity, ToolIdentity
from core.tools.errors import ToolInvokeError
from core.tools.workflow_as_tool.tool import WorkflowTool


def test_workflow_tool_should_raise_tool_invoke_error_when_result_has_error_field(monkeypatch):
    """Ensure that WorkflowTool will throw a `ToolInvokeError` exception when
    `WorkflowAppGenerator.generate` returns a result with `error` key inside
    the `data` element.
    """
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="test_tool", invoke_from=InvokeFrom.EXPLORE)
    tool = WorkflowTool(
        workflow_app_id="",
        workflow_as_tool_id="",
        version="1",
        workflow_entities={},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )

    # needs to patch those methods to avoid database access.
    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: None)
    monkeypatch.setattr(tool, "_get_workflow", lambda *args, **kwargs: None)

    # replace `WorkflowAppGenerator.generate` 's return value.
    monkeypatch.setattr(
        "core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate",
        lambda *args, **kwargs: {"data": {"error": "oops"}},
    )
    monkeypatch.setattr("flask_login.current_user", lambda *args, **kwargs: None)

    with pytest.raises(ToolInvokeError) as exc_info:
        # WorkflowTool always returns a generator, so we need to iterate to
        # actually `run` the tool.
        list(tool.invoke("test_user", {}))
    assert exc_info.value.args == ("oops",)


def test_workflow_tool_forwards_private_parent_trace_context(monkeypatch):
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="test_tool", invoke_from=InvokeFrom.EXPLORE)
    tool = WorkflowTool(
        workflow_app_id="",
        workflow_as_tool_id="",
        version="1",
        workflow_entities={},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )
    captured = {}

    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: object())
    monkeypatch.setattr(tool, "_get_workflow", lambda *args, **kwargs: object())
    monkeypatch.setattr(tool, "_transform_args", lambda tool_parameters: (tool_parameters, []))
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())

    def fake_generate(self, **kwargs):
        captured.update(kwargs)
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    tool.set_parent_trace_context(
        parent_workflow_run_id="outer-run",
        parent_node_execution_id="outer-run:1",
    )

    list(tool.invoke("test-user", {"parent_trace_context": "user-input"}))

    assert captured["args"]["inputs"] == {"parent_trace_context": "user-input"}
    assert captured["args"]["parent_trace_context"] == ParentTraceContext(
        parent_workflow_run_id="outer-run",
        parent_node_execution_id="outer-run:1",
    )


def test_workflow_tool_forwards_trace_session_id_to_child_generator(monkeypatch):
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="test_tool", invoke_from=InvokeFrom.EXPLORE)
    tool = WorkflowTool(
        workflow_app_id="",
        workflow_as_tool_id="",
        version="1",
        workflow_entities={},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )
    captured = {}

    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())

    def fake_generate(self, **kwargs):
        captured.update(kwargs)
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    tool.set_trace_session_id("external-session")
    forked_tool = tool.fork_tool_runtime(runtime)
    monkeypatch.setattr(forked_tool, "_get_app", lambda *args, **kwargs: object())
    monkeypatch.setattr(forked_tool, "_get_workflow", lambda *args, **kwargs: object())
    monkeypatch.setattr(forked_tool, "_transform_args", lambda tool_parameters: (tool_parameters, []))

    list(forked_tool.invoke("test-user", {"query": "hello"}))

    assert captured["args"]["inputs"] == {"query": "hello"}
    assert captured["args"]["trace_session_id"] == "external-session"
