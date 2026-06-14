from contextlib import contextmanager

import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.ops.trace_context import ParentTraceContext
from core.tools.__base.tool_runtime import ToolRuntime
from core.tools.entities.common_entities import I18nObject
from core.tools.entities.tool_entities import ToolEntity, ToolIdentity
from core.tools.errors import ToolInvokeError
from core.tools.workflow_as_tool.tool import WorkflowTool
from models.model import App
from models.workflow import Workflow


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


def _workflow_tool_for_session_tests():
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="tenant-1", invoke_from=InvokeFrom.EXPLORE)
    return WorkflowTool(
        workflow_app_id="app-1",
        workflow_as_tool_id="provider-1",
        version="1",
        workflow_entities={},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )


def test_workflow_tool_loads_app_with_short_session(monkeypatch):
    closed = {"value": False}
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            closed["value"] = True

        @contextmanager
        def begin(self):
            yield self

        def scalar(self, stmt):
            return app

        def expunge(self, instance):
            instance._detached_by_test = True

    monkeypatch.setattr("core.tools.workflow_as_tool.tool.session_factory.create_session", lambda: FakeSession())

    loaded = _workflow_tool_for_session_tests()._get_app("app-1")

    assert loaded is app
    assert loaded._detached_by_test is True
    assert closed["value"] is True


def test_workflow_tool_loads_workflow_with_short_session(monkeypatch):
    closed = {"value": False}
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")

    class FakeScalars:
        def first(self):
            return workflow

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            closed["value"] = True

        @contextmanager
        def begin(self):
            yield self

        def scalar(self, stmt):
            return workflow

        def scalars(self, stmt):
            return FakeScalars()

        def expunge(self, instance):
            instance._detached_by_test = True

    monkeypatch.setattr("core.tools.workflow_as_tool.tool.session_factory.create_session", lambda: FakeSession())

    loaded = _workflow_tool_for_session_tests()._get_workflow("app-1", "1")

    assert loaded is workflow
    assert loaded._detached_by_test is True
    assert closed["value"] is True


def test_workflow_tool_does_not_hold_lookup_session_while_child_workflow_runs(monkeypatch):
    closed_count = {"value": 0}
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")

    class FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            closed_count["value"] += 1

        @contextmanager
        def begin(self):
            yield self

        def scalar(self, stmt):
            return app if closed_count["value"] == 0 else workflow

        def expunge(self, instance):
            return None

    monkeypatch.setattr("core.tools.workflow_as_tool.tool.session_factory.create_session", lambda: FakeSession())
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())

    captured = {}

    def fake_generate(self, **kwargs):
        captured["closed_before_generate"] = closed_count["value"]
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    list(_workflow_tool_for_session_tests().invoke("user-1", {"query": "hello"}))

    assert captured["closed_before_generate"] == 2


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
