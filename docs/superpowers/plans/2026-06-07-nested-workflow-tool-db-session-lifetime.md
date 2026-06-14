# Nested Workflow-as-Tool DB Session Lifetime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Backport the Dify 1.13 workflow-as-tool database session lifetime fix so parent workflows do not hold read-only `tool_workflow_providers` / app / workflow transactions while waiting for child workflow execution.

**Architecture:** Introduce a small SQLAlchemy `session_factory` helper and use short-lived explicit sessions for workflow-as-tool provider, app, workflow, and user lookups. Query and eagerly read the fields needed to construct `WorkflowTool` metadata, detach returned `App` / `Workflow` / user objects before invoking `WorkflowAppGenerator.generate(...)`, and avoid global Flask-SQLAlchemy `db.session` in the workflow-as-tool invocation path. Preserve existing ActiveMQ fail-open/no DB fallback behavior; this fix is independent of the async node-log publisher.

**Tech Stack:** Python, Flask-SQLAlchemy, SQLAlchemy ORM `Session`, Pydantic tool entities, pytest, unittest.mock.

---

## Evidence and Upstream Reference

Pressure-test symptom in the current branch:

```text
sqlalchemy.exc.TimeoutError: QueuePool limit of size 20 overflow 0 reached, connection timed out, timeout 30.00
```

Live DB evidence during nested workflow-as-tool load:

```text
idle in transaction | 52
SELECT tool_workflow_providers... | 32
```

Likely current path:

```text
ToolNode._run
  -> ToolManager.get_workflow_tool_runtime(...)
      -> ToolManager.get_tool_runtime(... WORKFLOW ...)
          -> db.session.query(WorkflowToolProvider) ...
          -> ToolTransformService.workflow_provider_to_controller(...)
          -> WorkflowToolProviderController.from_db(...)
              -> db_provider.app / db_provider.user lazy relationship access
              -> WorkflowToolProviderController._get_db_provider_tool(...)
                  -> db.session.query(Workflow) ...
  -> ToolEngine.generic_invoke(...)
      -> WorkflowTool._invoke(...)
          -> WorkflowTool._get_app(...) using global db.session
          -> WorkflowTool._get_workflow(...) using global db.session
          -> child WorkflowAppGenerator.generate(...)
```

Dify 1.13 contains the relevant upstream fixes:

```text
759a932bb7 Fix: release WorkflowTool database sessions promptly (#26893)
37c2f3d4b6 fix: fix instance is not bind to session (#30913)
```

The target backport should follow the newer 1.13.3 shape: `session_factory.create_session()` plus `session.expunge(...)` for ORM objects that must outlive the short session.

## File Structure

Create:

- `api/core/db/session_factory.py` — small global SQLAlchemy sessionmaker wrapper used outside request-scoped Flask `db.session`.
- `api/tests/unit_tests/core/db/test_session_factory.py` — verifies configuration and clear error when not configured.
- `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py` — verifies provider construction uses explicit short sessions and does not use global `db.session` queries.

Modify:

- `api/extensions/ext_database.py` or API startup extension setup file that initializes `db.engine` — configure `session_factory` with the Flask-SQLAlchemy engine after the app initializes.
- `api/core/tools/workflow_as_tool/provider.py` — load provider/app/user/workflow through short explicit sessions; pass `session` into `_get_db_provider_tool()`; stop lazy relationship reads from the caller's `db_provider`.
- `api/core/tools/workflow_as_tool/tool.py` — replace global `db.session` lookups in `_get_app()` and `_get_workflow()` with short sessions and `expunge()`.
- `api/core/tools/tool_manager.py` — replace workflow provider lookup in `get_tool_runtime(... WORKFLOW ...)` with a short explicit session and avoid using the detached provider object after session close except scalar fields needed before close.
- `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py` — verify `_get_app()` / `_get_workflow()` close their sessions before child `generate(...)` runs.
- `api/tests/unit_tests/core/tools/test_tool_manager.py` or create this file if absent — verify workflow provider lookup no longer uses global `db.session` in `get_tool_runtime(... WORKFLOW ...)`.

Do not modify:

- `api/core/workflow/log_publisher/*` — ActiveMQ publisher reliability is already separate.
- `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py` — node-log async behavior is not part of this DB session fix.

## Task 1: Add and Configure `session_factory`

**Files:**

- Create: `api/core/db/session_factory.py`
- Modify: `api/extensions/ext_database.py` or the app initialization file that owns `db.init_app(app)`
- Test: `api/tests/unit_tests/core/db/test_session_factory.py`

- [ ] **Step 1: Write the failing session factory tests**

Create `api/tests/unit_tests/core/db/test_session_factory.py`:

```python
import pytest
from sqlalchemy import create_engine, text

from core.db import session_factory as session_factory_module
from core.db.session_factory import configure_session_factory, create_session, get_session_maker


def test_create_session_requires_configuration(monkeypatch):
    monkeypatch.setattr(session_factory_module, "_session_maker", None)

    with pytest.raises(RuntimeError, match="Session factory not configured"):
        create_session()


def test_configure_session_factory_creates_working_sessions(monkeypatch):
    monkeypatch.setattr(session_factory_module, "_session_maker", None)
    engine = create_engine("sqlite:///:memory:")

    configure_session_factory(engine, expire_on_commit=False)

    maker = get_session_maker()
    with maker() as session:
        assert session.execute(text("select 1")).scalar_one() == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/db/test_session_factory.py -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'core.db'` or `No module named 'core.db.session_factory'`.

- [ ] **Step 3: Implement `session_factory`**

Create `api/core/db/session_factory.py`:

```python
from __future__ import annotations

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

_session_maker: sessionmaker[Session] | None = None


def configure_session_factory(engine: Engine, expire_on_commit: bool = False) -> None:
    global _session_maker
    _session_maker = sessionmaker(bind=engine, expire_on_commit=expire_on_commit)


def get_session_maker() -> sessionmaker[Session]:
    if _session_maker is None:
        raise RuntimeError("Session factory not configured. Call configure_session_factory() first.")
    return _session_maker


def create_session() -> Session:
    return get_session_maker()()


class SessionFactory:
    @staticmethod
    def configure(engine: Engine, expire_on_commit: bool = False) -> None:
        configure_session_factory(engine, expire_on_commit)

    @staticmethod
    def get_session_maker() -> sessionmaker[Session]:
        return get_session_maker()

    @staticmethod
    def create_session() -> Session:
        return create_session()


session_factory = SessionFactory()
```

If `api/core/db/__init__.py` does not exist, create it:

```python
from core.db.session_factory import SessionFactory, configure_session_factory, create_session, get_session_maker

__all__ = ["SessionFactory", "configure_session_factory", "create_session", "get_session_maker"]
```

- [ ] **Step 4: Configure the factory during app startup**

Find where `db.init_app(app)` runs:

```bash
cd api && rg -n "db\.init_app|init_app\(app\)" extensions app.py .
```

In the file that initializes the database extension, add this import:

```python
from core.db.session_factory import session_factory
```

Immediately after `db.init_app(app)` and after an application context can access `db.engine`, add:

```python
    with app.app_context():
        session_factory.configure(db.engine, expire_on_commit=False)
```

If that file already has an app context block for DB setup, put the `session_factory.configure(...)` call inside the existing block instead of creating a nested one.

- [ ] **Step 5: Run tests and commit**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/db/test_session_factory.py -v
```

Expected: PASS.

Commit:

```bash
git add api/core/db api/extensions/ext_database.py api/tests/unit_tests/core/db/test_session_factory.py
git commit -m "feat: add explicit SQLAlchemy session factory"
```

If the startup hook is not in `api/extensions/ext_database.py`, replace that path in `git add` with the actual file changed.

## Task 2: Make `WorkflowTool._get_app()` and `_get_workflow()` Use Short Sessions

**Files:**

- Modify: `api/core/tools/workflow_as_tool/tool.py`
- Modify: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`

- [ ] **Step 1: Write failing tests proving child generation runs after lookup sessions close**

Append to `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`:

```python
from contextlib import contextmanager
from types import SimpleNamespace

from models.model import App
from models.workflow import Workflow


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_tool.py -v
```

Expected: FAIL because `core.tools.workflow_as_tool.tool.session_factory` is not imported and `_get_app()` / `_get_workflow()` still use global `db.session`.

- [ ] **Step 3: Implement short-session app/workflow lookup**

In `api/core/tools/workflow_as_tool/tool.py`, replace this import:

```python
from extensions.ext_database import db
```

with:

```python
from sqlalchemy import select

from core.db.session_factory import session_factory
```

Update `_get_workflow()`:

```python
    def _get_workflow(self, app_id: str, version: str) -> Workflow:
        """
        get the workflow by app id and version
        """
        with session_factory.create_session() as session, session.begin():
            if not version:
                stmt = (
                    select(Workflow)
                    .where(Workflow.app_id == app_id, Workflow.version != "draft")
                    .order_by(Workflow.created_at.desc())
                )
                workflow = session.scalars(stmt).first()
            else:
                stmt = select(Workflow).where(Workflow.app_id == app_id, Workflow.version == version)
                workflow = session.scalar(stmt)

            if not workflow:
                raise ValueError("workflow not found or not published")

            session.expunge(workflow)
            return workflow
```

Update `_get_app()`:

```python
    def _get_app(self, app_id: str) -> App:
        """
        get the app by app id
        """
        stmt = select(App).where(App.id == app_id)
        with session_factory.create_session() as session, session.begin():
            app = session.scalar(stmt)
            if not app:
                raise ValueError("app not found")

            session.expunge(app)
            return app
```

Keep `current_user`, trace context, `workflow_thread_pool_id`, and output behavior unchanged.

- [ ] **Step 4: Run tests and commit**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_tool.py -v
```

Expected: PASS.

Commit:

```bash
git add api/core/tools/workflow_as_tool/tool.py api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py
git commit -m "fix: release workflow tool app workflow sessions"
```

## Task 3: Make `WorkflowToolProviderController` Use Short Sessions

**Files:**

- Modify: `api/core/tools/workflow_as_tool/provider.py`
- Create: `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py`

- [ ] **Step 1: Write failing provider controller tests**

Create `api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py`:

```python
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

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
        parameter_configurations=[],
    )


def test_from_db_reloads_provider_with_short_session(monkeypatch):
    provider = _provider_row()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    user = Account(id="account-1", name="Alice", email="alice@example.com")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
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

        def get(self, model, primary_key):
            if model is WorkflowToolProvider:
                return provider
            if model is App:
                return app
            if model is Account:
                return user
            return None

        def query(self, model):
            assert model is Workflow
            return self

        def where(self, *args):
            return self

        def first(self):
            return workflow

    monkeypatch.setattr("core.tools.workflow_as_tool.provider.session_factory.create_session", lambda: FakeSession())
    monkeypatch.setattr(
        "core.tools.workflow_as_tool.provider.db.session.query",
        lambda *args, **kwargs: global_session_used.__setitem__("value", True),
    )

    controller = WorkflowToolProviderController.from_db(provider)

    assert controller.provider_id == "provider-1"
    assert controller.tools[0].workflow_app_id == "app-1"
    assert controller.tools[0].version == "1"
    assert closed["value"] is True
    assert global_session_used["value"] is False


def test_get_tools_uses_provider_id_not_app_id_and_short_session(monkeypatch):
    controller = WorkflowToolProviderController(
        entity=SimpleNamespace(identity=SimpleNamespace(name="Child"), credentials_schema=[], plugin_id=None),
        provider_id="provider-1",
    )
    controller.tools = None
    provider = _provider_row()
    app = App(id="app-1", tenant_id="tenant-1", mode="workflow", name="Child")
    workflow = Workflow(id="workflow-1", app_id="app-1", version="1", graph="{}", features="{}")
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
        lambda self, db_provider, app, session, user=None: SimpleNamespace(workflow_app_id=app.id),
    )

    tools = controller.get_tools("tenant-1")

    assert tools[0].workflow_app_id == "app-1"
    assert any("tool_workflow_providers.id" in item for item in where_text)
    assert not any("tool_workflow_providers.app_id" in item for item in where_text)
```

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider.py -v
```

Expected: FAIL because `provider.py` does not import `session_factory`, uses lazy relationships, and `get_tools()` filters by `app_id == self.provider_id`.

- [ ] **Step 3: Implement short-session provider construction**

In `api/core/tools/workflow_as_tool/provider.py`, add imports:

```python
from sqlalchemy.orm import Session

from core.db.session_factory import session_factory
from models.account import Account
```

Keep `from extensions.ext_database import db` only if another function still needs `db.engine`; do not use `db.session` in this file after the change.

Replace `from_db()` with:

```python
    @classmethod
    def from_db(cls, db_provider: WorkflowToolProvider) -> "WorkflowToolProviderController":
        with session_factory.create_session() as session, session.begin():
            provider = session.get(WorkflowToolProvider, db_provider.id) if db_provider.id else None
            if not provider:
                raise ValueError("workflow provider not found")

            app = session.get(App, provider.app_id)
            if not app:
                raise ValueError("app not found")

            user = session.get(Account, provider.user_id) if provider.user_id else None
            controller = WorkflowToolProviderController(
                entity=ToolProviderEntity(
                    identity=ToolProviderIdentity(
                        author=user.name if user else "",
                        name=provider.label,
                        label=I18nObject(en_US=provider.label, zh_Hans=provider.label),
                        description=I18nObject(en_US=provider.description, zh_Hans=provider.description),
                        icon=provider.icon,
                    ),
                    credentials_schema=[],
                    plugin_id=None,
                ),
                provider_id=provider.id or "",
            )
            controller.tools = [controller._get_db_provider_tool(provider, app, session=session, user=user)]

        return controller
```

Change `_get_db_provider_tool()` signature:

```python
    def _get_db_provider_tool(
        self,
        db_provider: WorkflowToolProvider,
        app: App,
        *,
        session: Session,
        user: Account | None = None,
    ) -> WorkflowTool:
```

Inside `_get_db_provider_tool()`, replace the workflow query with the provided session:

```python
        workflow: Workflow | None = (
            session.query(Workflow)
            .where(Workflow.app_id == db_provider.app_id, Workflow.version == db_provider.version)
            .first()
        )
```

Remove this line:

```python
        user = db_provider.user
```

Update `get_tools()` to use the provider id and the short session:

```python
    def get_tools(self, tenant_id: str) -> list[WorkflowTool]:
        if self.tools is not None:
            return self.tools

        with session_factory.create_session() as session, session.begin():
            db_provider: WorkflowToolProvider | None = (
                session.query(WorkflowToolProvider)
                .where(
                    WorkflowToolProvider.tenant_id == tenant_id,
                    WorkflowToolProvider.id == self.provider_id,
                )
                .first()
            )
            if not db_provider:
                return []

            app = session.get(App, db_provider.app_id)
            if not app:
                raise ValueError("app not found")

            user = session.get(Account, db_provider.user_id) if db_provider.user_id else None
            self.tools = [self._get_db_provider_tool(db_provider, app, session=session, user=user)]

        return self.tools
```

- [ ] **Step 4: Run tests and commit**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/workflow_as_tool/test_provider.py tests/unit_tests/core/tools/workflow_as_tool/test_tool.py -v
```

Expected: PASS.

Commit:

```bash
git add api/core/tools/workflow_as_tool/provider.py api/tests/unit_tests/core/tools/workflow_as_tool/test_provider.py
git commit -m "fix: release workflow tool provider sessions"
```

## Task 4: Make `ToolManager.get_tool_runtime(... WORKFLOW ...)` Use a Short Session

**Files:**

- Modify: `api/core/tools/tool_manager.py`
- Create or Modify: `api/tests/unit_tests/core/tools/test_tool_manager.py`

- [ ] **Step 1: Write a failing ToolManager test**

If `api/tests/unit_tests/core/tools/test_tool_manager.py` does not exist, create it. Add:

```python
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from core.app.entities.app_invoke_entities import InvokeFrom
from core.tools.entities.tool_entities import ToolProviderType, ToolInvokeFrom
from core.tools.errors import ToolProviderNotFoundError
from core.tools.tool_manager import ToolManager
from models.tools import WorkflowToolProvider


def test_get_tool_runtime_workflow_provider_lookup_uses_short_session(monkeypatch):
    provider = WorkflowToolProvider(
        id="provider-1",
        tenant_id="tenant-1",
        app_id="app-1",
        name="child_workflow",
        label="Child Workflow",
        description="Child workflow as tool",
        icon="{}",
        version="1",
        parameter_configurations=[],
    )
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

    class FakeController:
        def get_tools(self, tenant_id):
            assert tenant_id == "tenant-1"
            return [FakeWorkflowTool()]

    monkeypatch.setattr("core.tools.tool_manager.session_factory.create_session", lambda: FakeSession())
    monkeypatch.setattr(
        "core.tools.tool_manager.db.session.query",
        lambda *args, **kwargs: global_session_used.__setitem__("value", True),
    )
    monkeypatch.setattr(
        "core.tools.tool_manager.ToolTransformService.workflow_provider_to_controller",
        lambda db_provider: FakeController(),
    )

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
```

- [ ] **Step 2: Run the test to verify failure**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/test_tool_manager.py -v
```

Expected: FAIL because `core.tools.tool_manager.session_factory` is not imported and workflow provider lookup uses global `db.session`.

- [ ] **Step 3: Implement short-session workflow provider lookup**

In `api/core/tools/tool_manager.py`, add import:

```python
from core.db.session_factory import session_factory
```

Replace the `elif provider_type == ToolProviderType.WORKFLOW:` block with:

```python
        elif provider_type == ToolProviderType.WORKFLOW:
            with session_factory.create_session() as session, session.begin():
                workflow_provider = (
                    session.query(WorkflowToolProvider)
                    .filter(WorkflowToolProvider.tenant_id == tenant_id, WorkflowToolProvider.id == provider_id)
                    .first()
                )

                if workflow_provider is None:
                    raise ToolProviderNotFoundError(f"workflow provider {provider_id} not found")

                controller = ToolTransformService.workflow_provider_to_controller(db_provider=workflow_provider)
                provider_tenant_id = workflow_provider.tenant_id

            controller_tools: list[WorkflowTool] = controller.get_tools(tenant_id=provider_tenant_id)
            if controller_tools is None or len(controller_tools) == 0:
                raise ToolProviderNotFoundError(f"workflow provider {provider_id} not found")

            return cast(
                WorkflowTool,
                controller_tools[0].fork_tool_runtime(
                    runtime=ToolRuntime(
                        tenant_id=tenant_id,
                        credentials={},
                        invoke_from=invoke_from,
                        tool_invoke_from=tool_invoke_from,
                    )
                ),
            )
```

This intentionally keeps the session open while `workflow_provider_to_controller()` calls `WorkflowToolProviderController.from_db(...)`. After Task 3, `from_db()` reloads by id with its own short session and does not depend on the caller's lazy relationships. The outer session is closed before `controller.get_tools(...)`, runtime parameter conversion, and child workflow invocation.

- [ ] **Step 4: Run tests and commit**

Run:

```bash
cd api && uv run pytest -o addopts='' tests/unit_tests/core/tools/test_tool_manager.py tests/unit_tests/core/tools/workflow_as_tool -v
```

Expected: PASS.

Commit:

```bash
git add api/core/tools/tool_manager.py api/tests/unit_tests/core/tools/test_tool_manager.py
git commit -m "fix: release workflow provider lookup session"
```

## Task 5: Regression Verification for Nested Workflow-as-Tool Pressure

**Files:**

- No code changes expected unless verification finds issues.

- [ ] **Step 1: Run focused unit tests**

Run:

```bash
cd api && uv run pytest -o addopts='' \
  tests/unit_tests/core/db/test_session_factory.py \
  tests/unit_tests/core/tools/test_tool_manager.py \
  tests/unit_tests/core/tools/workflow_as_tool \
  tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py \
  -v
```

Expected: PASS.

- [ ] **Step 2: Run import smoke test**

Run:

```bash
cd api && uv run python - <<'PY'
from core.db.session_factory import session_factory
from core.tools.tool_manager import ToolManager
from core.tools.workflow_as_tool.provider import WorkflowToolProviderController
from core.tools.workflow_as_tool.tool import WorkflowTool
print('ok')
PY
```

Expected: prints `ok`.

- [ ] **Step 3: Run manual nested workflow pressure test**

Prerequisites:

```text
- API server running with SQLAlchemy pool size 20 / overflow 0, matching the observed failure environment.
- A parent workflow exposed through Service API.
- The parent workflow invokes a child workflow through workflow-as-tool.
- Parent and child workflows can be minimal start/end workflows to isolate nesting overhead.
- PostgreSQL access available for pg_stat_activity queries.
```

Before the pressure run, start a DB watch in another terminal:

```sql
SELECT state, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state
ORDER BY count DESC;

SELECT state, left(query, 80) AS query_prefix, count(*)
FROM pg_stat_activity
WHERE datname = current_database()
  AND query ILIKE '%tool_workflow_providers%'
GROUP BY state, left(query, 80)
ORDER BY count DESC;
```

Run the pressure test twice:

```bash
export DIFY_WORKFLOW_API_TOKEN='set-to-parent-workflow-api-token'
export DIFY_WORKFLOW_RUN_URL='http://localhost:5001/v1/workflows/run'
test -f payload.json
hey -n 1000 -c 50 -m POST \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${DIFY_WORKFLOW_API_TOKEN}" \
  -d @payload.json \
  "${DIFY_WORKFLOW_RUN_URL}"
hey -n 1000 -c 50 -m POST \
  -H 'Content-Type: application/json' \
  -H "Authorization: Bearer ${DIFY_WORKFLOW_API_TOKEN}" \
  -d @payload.json \
  "${DIFY_WORKFLOW_RUN_URL}"
```

Expected:

```text
- No sqlalchemy.exc.TimeoutError from QueuePool exhaustion.
- pg_stat_activity does not accumulate idle in transaction rows whose last query is SELECT tool_workflow_providers...
- Parent workflow requests complete successfully under the same concurrency that previously failed.
- ActiveMQ node-log async behavior remains fail-open and does not fall back to synchronous node DB writes.
```

- [ ] **Step 4: Commit verification-only fixes if needed**

If verification requires small code/test fixes, run:

```bash
git status --short
git add api/core/db api/core/tools api/tests/unit_tests/core/db api/tests/unit_tests/core/tools api/tests/unit_tests/core/workflow/nodes/tool
git commit -m "fix: stabilize workflow tool session lifetime"
```

Do not create an empty commit when there are no verification fixes.

---

## Self-Review Notes

Spec coverage:

- The observed `idle in transaction | SELECT tool_workflow_providers...` cluster is addressed by Tasks 3 and 4.
- `WorkflowTool._get_app()` / `_get_workflow()` global session usage before child workflow execution is addressed by Task 2.
- The Dify 1.13 `session_factory + expunge()` approach is captured in Tasks 1 and 2.
- Provider lazy relationship access (`db_provider.app`, `db_provider.user`) is removed in Task 3.
- The current `get_tools()` `app_id == self.provider_id` issue is corrected to `WorkflowToolProvider.id == self.provider_id` in Task 3, matching 1.13 behavior.
- ActiveMQ producer/consumer behavior is intentionally out of scope and should not be modified by this plan.
- Regression verification includes both unit tests and the nested workflow pressure scenario that exposed the issue.

No placeholders remain; each task has exact file paths, code shapes, commands, expected results, and commit points.
