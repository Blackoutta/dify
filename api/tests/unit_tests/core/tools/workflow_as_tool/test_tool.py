from contextlib import contextmanager

import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.ops.trace_context import ParentTraceContext
from core.tools.__base.tool_runtime import ToolRuntime
from core.tools.entities.common_entities import I18nObject
from core.tools.entities.tool_entities import ToolEntity, ToolIdentity, ToolParameter
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
    monkeypatch.setattr(
        tool,
        "_get_app",
        lambda *args, **kwargs: App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
    )
    monkeypatch.setattr(
        tool,
        "_get_workflow",
        lambda *args, **kwargs: Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}"),
    )

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

    monkeypatch.setattr(
        tool,
        "_get_app",
        lambda *args, **kwargs: App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
    )
    monkeypatch.setattr(
        tool,
        "_get_workflow",
        lambda *args, **kwargs: Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}"),
    )
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
    monkeypatch.setattr(
        forked_tool,
        "_get_app",
        lambda *args, **kwargs: App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child"),
    )
    monkeypatch.setattr(
        forked_tool,
        "_get_workflow",
        lambda *args, **kwargs: Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}"),
    )
    monkeypatch.setattr(forked_tool, "_transform_args", lambda tool_parameters: (tool_parameters, []))

    list(forked_tool.invoke("test-user", {"query": "hello"}))

    assert captured["args"]["inputs"] == {"query": "hello"}
    assert captured["args"]["trace_session_id"] == "external-session"


def test_workflow_tool_invoke_uses_cached_app_and_workflow_entities(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": app, "workflow": workflow}
    captured = {}

    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("app reloaded")))
    monkeypatch.setattr(
        tool,
        "_get_workflow",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("workflow reloaded")),
    )
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())

    def fake_generate(self, **kwargs):
        captured.update(kwargs)
        return {"data": {"outputs": {"answer": "ok"}}}

    monkeypatch.setattr("core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate", fake_generate)

    list(tool.invoke("user-1", {"query": "hello"}))

    assert captured["app_model"] is app
    assert captured["workflow"] is workflow


def test_workflow_tool_invoke_loads_only_missing_workflow_entity(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": app}
    calls = {"app": 0, "workflow": 0}

    def fail_get_app(*args, **kwargs):
        calls["app"] += 1
        raise AssertionError("app should not reload")

    def fake_get_workflow(*args, **kwargs):
        calls["workflow"] += 1
        return workflow

    monkeypatch.setattr(tool, "_get_app", fail_get_app)
    monkeypatch.setattr(tool, "_get_workflow", fake_get_workflow)
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())
    monkeypatch.setattr(
        "core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate",
        lambda self, **kwargs: {"data": {"outputs": {"answer": "ok"}}},
    )

    list(tool.invoke("user-1", {}))

    assert calls == {"app": 0, "workflow": 1}


def test_workflow_tool_invoke_loads_only_missing_app_entity(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"workflow": workflow}
    calls = {"app": 0, "workflow": 0}

    def fake_get_app(*args, **kwargs):
        calls["app"] += 1
        return app

    def fail_get_workflow(*args, **kwargs):
        calls["workflow"] += 1
        raise AssertionError("workflow should not reload")

    monkeypatch.setattr(tool, "_get_app", fake_get_app)
    monkeypatch.setattr(tool, "_get_workflow", fail_get_workflow)
    monkeypatch.setattr("core.tools.workflow_as_tool.tool.current_user", object())
    monkeypatch.setattr(
        "core.app.apps.workflow.app_generator.WorkflowAppGenerator.generate",
        lambda self, **kwargs: {"data": {"outputs": {"answer": "ok"}}},
    )

    list(tool.invoke("user-1", {}))

    assert calls == {"app": 1, "workflow": 0}


def test_workflow_tool_invoke_rejects_invalid_present_cached_entities(monkeypatch):
    tool = _workflow_tool_for_session_tests()
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    tool.workflow_entities = {"app": None, "workflow": workflow}

    monkeypatch.setattr(tool, "_get_app", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no fallback")))
    monkeypatch.setattr(
        tool,
        "_get_workflow",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("no fallback")),
    )

    with pytest.raises(ValueError, match="invalid cached workflow tool app"):
        list(tool.invoke("user-1", {}))


def test_workflow_tool_fork_deep_copies_entity_parameters_and_copies_workflow_entities_dict():
    parameter = ToolParameter(
        name="query",
        label=I18nObject(en_US="Query"),
        human_description=I18nObject(en_US="Query"),
        type=ToolParameter.ToolParameterType.STRING,
        form=ToolParameter.ToolParameterForm.LLM,
        llm_description="Query",
        required=True,
    )
    entity = ToolEntity(
        identity=ToolIdentity(author="test", name="test tool", label=I18nObject(en_US="test tool"), provider="test"),
        parameters=[parameter],
        description=None,
        output_schema=None,
        has_runtime_parameters=False,
    )
    runtime = ToolRuntime(tenant_id="tenant-1", invoke_from=InvokeFrom.EXPLORE)
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
    prototype = WorkflowTool(
        workflow_app_id="app-1",
        workflow_as_tool_id="provider-1",
        version="1",
        workflow_entities={"app": app, "workflow": workflow},
        workflow_call_depth=1,
        entity=entity,
        runtime=runtime,
    )

    fork1 = prototype.fork_tool_runtime(runtime)
    fork2 = prototype.fork_tool_runtime(runtime)

    fork1.entity.parameters[0].name = "changed"
    fork1.workflow_entities["app"] = App(id="other-app", tenant_id="tenant-1", mode="workflow", name="Other")

    assert prototype.entity.parameters[0].name == "query"
    assert fork2.entity.parameters[0].name == "query"
    assert prototype.workflow_entities["app"] is app
    assert fork2.workflow_entities["app"] is app
    assert fork1.workflow_entities is not prototype.workflow_entities
