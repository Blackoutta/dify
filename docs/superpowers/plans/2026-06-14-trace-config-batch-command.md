# Trace Config Batch Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `flask trace-config` command group that can list trace providers, generate provider input templates, and batch upsert trace provider configuration for explicit app IDs.

**Architecture:** Keep the feature isolated in a new service module (`api/services/trace_config_batch_service.py`) and a new Click command module (`api/commands/trace_config.py`). The service owns provider normalization, schema/template generation, validation, encrypted upsert, per-app transactions, and structured results; the CLI owns parsing, prompting, deterministic output, and exit codes.

**Tech Stack:** Python 3.12, Flask CLI / Click, SQLAlchemy via `extensions.ext_database.db`, Pydantic v2 provider config models, PyYAML, pytest, `click.testing.CliRunner`.

---

## File Structure

- Create `api/services/trace_config_batch_service.py`
  - Dataclasses: `AppBatchResult`, `BatchResult`
  - Exception: `TraceConfigBatchError`
  - Service: `TraceConfigBatchService`
  - Responsibilities: provider list/template generation, input validation, external validation, per-app upsert/enabling, wizard workspace/app lookup, summary results.
- Create `api/commands/trace_config.py`
  - Click group `trace_config`
  - Subcommands: `providers`, `template`, `batch`, `wizard`
  - Helpers: JSON/YAML file parsing, app ID parsing, workspace/app selection prompts, safe output formatting.
- Move `api/commands.py` to `api/commands/__init__.py`
  - Preserve existing `from commands import ...` behavior while allowing `api/commands/trace_config.py`.
- Modify `api/extensions/ext_commands.py`
  - Import and register `trace_config` with the Flask app CLI.
- Create `api/tests/unit_tests/services/test_trace_config_batch_service.py`
  - Unit tests for service behavior with mocked database/session and tracing manager calls.
- Create `api/tests/unit_tests/commands/test_trace_config_command.py`
  - CLI tests using `CliRunner` and mocked service methods.

---

### Task 1: Add service result contracts, provider helpers, and template generation

**Files:**
- Create: `api/services/trace_config_batch_service.py`
- Test: `api/tests/unit_tests/services/test_trace_config_batch_service.py`

- [ ] **Step 1: Write failing tests for provider listing and dynamic templates**

Create `api/tests/unit_tests/services/test_trace_config_batch_service.py` with these tests first:

```python
from core.ops.entities.config_entity import TracingProviderEnum
from services.trace_config_batch_service import TraceConfigBatchService


def test_list_providers_returns_all_supported_provider_values():
    providers = TraceConfigBatchService.list_providers()

    assert providers == sorted([provider.value for provider in TracingProviderEnum])


def test_get_template_marks_required_fields_and_uses_defaults():
    template = TraceConfigBatchService.get_template("langfuse")

    assert template == {
        "provider": "langfuse",
        "app_ids": ["<app-id>"],
        "tracing_config": {
            "public_key": "<required>",
            "secret_key": "<required>",
            "host": "https://api.langfuse.com",
        },
        "enable": True,
    }


def test_get_all_templates_contains_each_supported_provider():
    templates = TraceConfigBatchService.get_all_templates()

    assert set(templates.keys()) == {provider.value for provider in TracingProviderEnum}
    assert templates["aliyun"]["tracing_config"]["license_key"] == "<required>"
    assert templates["aliyun"]["tracing_config"]["endpoint"] == "<required>"
    assert templates["aliyun"]["tracing_config"]["app_name"] == "dify_app"
```

- [ ] **Step 2: Run tests to verify they fail because the service does not exist**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'services.trace_config_batch_service'`.

- [ ] **Step 3: Add the initial service module**

Create `api/services/trace_config_batch_service.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic_core import PydanticUndefined

from core.ops.entities.config_entity import TracingProviderEnum
from core.ops.ops_trace_manager import provider_config_map


class TraceConfigBatchError(Exception):
    """Raised for batch-level trace config failures before per-app processing starts."""


@dataclass(slots=True)
class AppBatchResult:
    app_id: str
    status: str
    enabled: bool = False
    app_name: str | None = None
    error: str | None = None


@dataclass(slots=True)
class BatchResult:
    provider: str
    enabled: bool
    validation_skipped: bool
    results: list[AppBatchResult] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def succeeded(self) -> int:
        return sum(1 for result in self.results if result.error is None)

    @property
    def failed(self) -> int:
        return sum(1 for result in self.results if result.error is not None)

    @property
    def has_failures(self) -> bool:
        return self.failed > 0


class TraceConfigBatchService:
    @classmethod
    def list_providers(cls) -> list[str]:
        return sorted(provider.value for provider in TracingProviderEnum)

    @classmethod
    def normalize_provider(cls, provider: str) -> str:
        normalized = (provider or "").strip().lower()
        if normalized not in cls.list_providers():
            raise TraceConfigBatchError(f"Unsupported tracing provider: {provider}")
        try:
            provider_config_map[normalized]
        except KeyError as exc:
            raise TraceConfigBatchError(f"Unsupported tracing provider: {provider}") from exc
        return normalized

    @classmethod
    def get_template(cls, provider: str) -> dict[str, Any]:
        normalized_provider = cls.normalize_provider(provider)
        config_class = provider_config_map[normalized_provider]["config_class"]
        tracing_config: dict[str, Any] = {}

        for field_name, model_field in config_class.model_fields.items():
            if model_field.is_required():
                tracing_config[field_name] = "<required>"
            elif model_field.default is PydanticUndefined:
                tracing_config[field_name] = "<required>"
            else:
                tracing_config[field_name] = model_field.default

        return {
            "provider": normalized_provider,
            "app_ids": ["<app-id>"],
            "tracing_config": tracing_config,
            "enable": True,
        }

    @classmethod
    def get_all_templates(cls) -> dict[str, dict[str, Any]]:
        return {provider: cls.get_template(provider) for provider in cls.list_providers()}
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: PASS for 3 tests.

- [ ] **Step 5: Commit**

```bash
git add api/services/trace_config_batch_service.py api/tests/unit_tests/services/test_trace_config_batch_service.py
git commit -m "feat: add trace config provider templates"
```

---

### Task 2: Add schema validation and external validation behavior

**Files:**
- Modify: `api/services/trace_config_batch_service.py`
- Modify: `api/tests/unit_tests/services/test_trace_config_batch_service.py`

- [ ] **Step 1: Add failing validation tests**

Append these tests to `api/tests/unit_tests/services/test_trace_config_batch_service.py`:

```python
from unittest.mock import patch

import pytest

from services.trace_config_batch_service import TraceConfigBatchError


def test_normalize_provider_rejects_unsupported_provider():
    with pytest.raises(TraceConfigBatchError, match="Unsupported tracing provider"):
        TraceConfigBatchService.normalize_provider("not-real")


def test_validate_tracing_config_rejects_invalid_schema():
    with pytest.raises(TraceConfigBatchError, match="Invalid tracing config"):
        TraceConfigBatchService.validate_tracing_config("langfuse", {"public_key": "pk-only"})


def test_validate_tracing_config_applies_provider_defaults():
    config = TraceConfigBatchService.validate_tracing_config(
        "langfuse",
        {"public_key": "pk", "secret_key": "sk"},
    )

    assert config == {
        "public_key": "pk",
        "secret_key": "sk",
        "host": "https://api.langfuse.com",
    }


def test_validate_credentials_skips_external_check_when_requested():
    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective") as api_check:
        TraceConfigBatchService.validate_credentials("langfuse", {"public_key": "pk", "secret_key": "sk"}, validate=False)

    api_check.assert_not_called()


def test_validate_credentials_calls_external_check_once_when_requested():
    with patch(
        "services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective",
        return_value=True,
    ) as api_check:
        TraceConfigBatchService.validate_credentials("langfuse", {"public_key": "pk", "secret_key": "sk"}, validate=True)

    api_check.assert_called_once_with(
        {"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        "langfuse",
    )


def test_validate_credentials_raises_when_external_check_fails():
    with patch(
        "services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective",
        return_value=False,
    ):
        with pytest.raises(TraceConfigBatchError, match="Invalid Credentials"):
            TraceConfigBatchService.validate_credentials(
                "langfuse",
                {"public_key": "pk", "secret_key": "sk"},
                validate=True,
            )
```

- [ ] **Step 2: Run tests to verify they fail on missing methods**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: FAIL with `AttributeError` for `validate_tracing_config` and `validate_credentials`.

- [ ] **Step 3: Implement schema and external validation**

Modify `api/services/trace_config_batch_service.py` imports:

```python
from pydantic import ValidationError
from pydantic_core import PydanticUndefined

from core.ops.ops_trace_manager import OpsTraceManager, provider_config_map
```

Add these methods to `TraceConfigBatchService`:

```python
    @classmethod
    def validate_tracing_config(cls, provider: str, tracing_config: dict[str, Any]) -> dict[str, Any]:
        normalized_provider = cls.normalize_provider(provider)
        config_class = provider_config_map[normalized_provider]["config_class"]
        try:
            return config_class(**tracing_config).model_dump()
        except ValidationError as exc:
            raise TraceConfigBatchError(f"Invalid tracing config for provider {normalized_provider}: {exc}") from exc

    @classmethod
    def validate_credentials(cls, provider: str, tracing_config: dict[str, Any], *, validate: bool) -> dict[str, Any]:
        normalized_provider = cls.normalize_provider(provider)
        validated_config = cls.validate_tracing_config(normalized_provider, tracing_config)
        if not validate:
            return validated_config
        if not OpsTraceManager.check_trace_config_is_effective(validated_config, normalized_provider):
            raise TraceConfigBatchError("Invalid Credentials")
        return validated_config
```

- [ ] **Step 4: Run tests to verify they pass**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: PASS for validation tests and existing template tests.

- [ ] **Step 5: Commit**

```bash
git add api/services/trace_config_batch_service.py api/tests/unit_tests/services/test_trace_config_batch_service.py
git commit -m "feat: validate trace config batch input"
```

---

### Task 3: Implement per-app upsert behavior without enabling tracing

**Files:**
- Modify: `api/services/trace_config_batch_service.py`
- Modify: `api/tests/unit_tests/services/test_trace_config_batch_service.py`

- [ ] **Step 1: Add failing per-app upsert tests with in-memory fakes**

Append to `api/tests/unit_tests/services/test_trace_config_batch_service.py`:

```python
from dataclasses import dataclass
from types import SimpleNamespace


@dataclass
class FakeTraceConfig:
    app_id: str
    tracing_provider: str
    tracing_config: dict


class FakeQuery:
    def __init__(self, model, session):
        self.model = model
        self.session = session
        self.filters = []

    def filter(self, *filters):
        self.filters.extend(filters)
        return self

    def first(self):
        if self.model.__name__ == "App":
            return self.session.apps.get(self.session.current_app_id)
        if self.model.__name__ == "TraceAppConfig":
            return self.session.trace_configs.get((self.session.current_app_id, self.session.current_provider))
        return None


class FakeSession:
    def __init__(self):
        self.current_app_id = None
        self.current_provider = None
        self.apps = {}
        self.trace_configs = {}
        self.added = []
        self.commits = 0
        self.rollbacks = 0

    def query(self, model):
        return FakeQuery(model, self)

    def add(self, item):
        self.added.append(item)
        self.trace_configs[(item.app_id, item.tracing_provider)] = item

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def install_fake_lookup(monkeypatch, fake_session):
    def fake_load_app(app_id):
        fake_session.current_app_id = app_id
        return fake_session.apps.get(app_id)

    def fake_load_config(app_id, provider):
        fake_session.current_app_id = app_id
        fake_session.current_provider = provider
        return fake_session.trace_configs.get((app_id, provider))

    monkeypatch.setattr("services.trace_config_batch_service.db.session", fake_session)
    monkeypatch.setattr(TraceConfigBatchService, "_load_app", staticmethod(fake_load_app))
    monkeypatch.setattr(TraceConfigBatchService, "_load_trace_config", staticmethod(fake_load_config))
    monkeypatch.setattr("services.trace_config_batch_service.TraceAppConfig", FakeTraceConfig)


def test_batch_upsert_creates_config_when_none_exists(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True), patch(
        "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
        return_value={"public_key": "encrypted-pk", "secret_key": "encrypted-sk", "host": "https://api.langfuse.com"},
    ) as encrypt_config:
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    encrypt_config.assert_called_once_with(
        "tenant-1",
        "langfuse",
        {"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        None,
    )
    assert result.succeeded == 1
    assert result.failed == 0
    assert result.results[0].status == "created"
    assert fake_session.trace_configs[("app-1", "langfuse")].tracing_config["public_key"] == "encrypted-pk"
    assert fake_session.commits == 1


def test_batch_upsert_updates_config_when_one_exists(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    fake_session.trace_configs[("app-1", "langfuse")] = FakeTraceConfig(
        app_id="app-1",
        tracing_provider="langfuse",
        tracing_config={"public_key": "old", "secret_key": "old", "host": "https://api.langfuse.com"},
    )
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True), patch(
        "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
        return_value={"public_key": "new", "secret_key": "new", "host": "https://api.langfuse.com"},
    ) as encrypt_config:
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    encrypt_config.assert_called_once_with(
        "tenant-1",
        "langfuse",
        {"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        {"public_key": "old", "secret_key": "old", "host": "https://api.langfuse.com"},
    )
    assert result.results[0].status == "updated"
    assert fake_session.trace_configs[("app-1", "langfuse")].tracing_config["public_key"] == "new"
    assert fake_session.commits == 1
```

- [ ] **Step 2: Run tests to verify they fail on missing `batch_upsert`**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: FAIL with `AttributeError: type object 'TraceConfigBatchService' has no attribute 'batch_upsert'`.

- [ ] **Step 3: Implement app loading, trace config loading, and upsert loop**

Modify imports in `api/services/trace_config_batch_service.py`:

```python
from extensions.ext_database import db
from models.model import App, TraceAppConfig
```

Add methods to `TraceConfigBatchService`:

```python
    @staticmethod
    def _load_app(app_id: str) -> App | None:
        return db.session.query(App).filter(App.id == app_id).first()

    @staticmethod
    def _load_trace_config(app_id: str, provider: str) -> TraceAppConfig | None:
        return (
            db.session.query(TraceAppConfig)
            .filter(TraceAppConfig.app_id == app_id, TraceAppConfig.tracing_provider == provider)
            .first()
        )

    @classmethod
    def _upsert_one_app(cls, app_id: str, provider: str, tracing_config: dict[str, Any], *, enable: bool) -> AppBatchResult:
        app = cls._load_app(app_id)
        if app is None:
            raise ValueError("App not found")

        current_config = cls._load_trace_config(app_id, provider)
        encrypted_config = OpsTraceManager.encrypt_tracing_config(
            app.tenant_id,
            provider,
            tracing_config,
            current_config.tracing_config if current_config else None,
        )

        if current_config:
            current_config.tracing_config = encrypted_config
            status = "updated"
        else:
            current_config = TraceAppConfig(
                app_id=app_id,
                tracing_provider=provider,
                tracing_config=encrypted_config,
            )
            db.session.add(current_config)
            status = "created"

        if enable:
            OpsTraceManager.update_app_tracing_config(app_id, True, provider)
            status = f"{status}, enabled"

        db.session.commit()
        return AppBatchResult(app_id=app_id, app_name=getattr(app, "name", None), status=status, enabled=enable)

    @classmethod
    def batch_upsert(
        cls,
        provider: str,
        app_ids: list[str],
        tracing_config: dict[str, Any],
        enable: bool = True,
        validate: bool = True,
        fail_fast: bool = False,
    ) -> BatchResult:
        normalized_provider = cls.normalize_provider(provider)
        if not app_ids:
            raise TraceConfigBatchError("At least one app ID is required")

        validated_config = cls.validate_credentials(normalized_provider, tracing_config, validate=validate)
        batch_result = BatchResult(
            provider=normalized_provider,
            enabled=enable,
            validation_skipped=not validate,
        )

        for app_id in app_ids:
            try:
                batch_result.results.append(
                    cls._upsert_one_app(app_id.strip(), normalized_provider, validated_config, enable=enable)
                )
            except Exception as exc:
                db.session.rollback()
                batch_result.results.append(AppBatchResult(app_id=app_id, status="failed", error=str(exc)))
                if fail_fast:
                    break

        return batch_result
```

- [ ] **Step 4: Run service tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: PASS for template, validation, and upsert tests.

- [ ] **Step 5: Commit**

```bash
git add api/services/trace_config_batch_service.py api/tests/unit_tests/services/test_trace_config_batch_service.py
git commit -m "feat: upsert trace configs in batch service"
```

---

### Task 4: Add enable/default behavior and per-app failure handling

**Files:**
- Modify: `api/services/trace_config_batch_service.py`
- Modify: `api/tests/unit_tests/services/test_trace_config_batch_service.py`

- [ ] **Step 1: Add failing tests for enable and failure modes**

Append tests:

```python

def test_batch_upsert_enables_app_tracing_by_default(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True), patch(
        "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
        return_value={"public_key": "encrypted-pk", "secret_key": "encrypted-sk", "host": "https://api.langfuse.com"},
    ), patch("services.trace_config_batch_service.OpsTraceManager.update_app_tracing_config") as update_tracing:
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
        )

    update_tracing.assert_called_once_with("app-1", True, "langfuse")
    assert result.results[0].enabled is True
    assert result.results[0].status == "created, enabled"


def test_batch_upsert_does_not_enable_when_enable_false(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True), patch(
        "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
        return_value={"public_key": "encrypted-pk", "secret_key": "encrypted-sk", "host": "https://api.langfuse.com"},
    ), patch("services.trace_config_batch_service.OpsTraceManager.update_app_tracing_config") as update_tracing:
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    update_tracing.assert_not_called()
    assert result.results[0].enabled is False
    assert result.results[0].status == "created"


def test_batch_upsert_continues_after_per_app_failure_by_default(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-2"] = SimpleNamespace(id="app-2", tenant_id="tenant-1", name="App Two", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True), patch(
        "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
        return_value={"public_key": "encrypted-pk", "secret_key": "encrypted-sk", "host": "https://api.langfuse.com"},
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["missing-app", "app-2"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    assert result.total == 2
    assert result.failed == 1
    assert result.succeeded == 1
    assert result.results[0].status == "failed"
    assert result.results[0].error == "App not found"
    assert result.results[1].status == "created"
    assert fake_session.rollbacks == 1


def test_batch_upsert_stops_after_first_per_app_failure_with_fail_fast(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-2"] = SimpleNamespace(id="app-2", tenant_id="tenant-1", name="App Two", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["missing-app", "app-2"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
            fail_fast=True,
        )

    assert result.total == 1
    assert result.failed == 1
    assert result.results[0].app_id == "missing-app"
```

- [ ] **Step 2: Run tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: If Task 3 implementation already includes enable/failure logic, these pass; otherwise failures identify exact missing behavior.

- [ ] **Step 3: Adjust implementation only if tests fail**

If needed, ensure `_upsert_one_app()` calls `OpsTraceManager.update_app_tracing_config(app_id, True, provider)` only when `enable=True`, and ensure `batch_upsert()` catches per-app exceptions, calls `db.session.rollback()`, appends `AppBatchResult(status="failed", error=str(exc))`, and breaks only when `fail_fast=True`.

- [ ] **Step 4: Run tests again**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/services/trace_config_batch_service.py api/tests/unit_tests/services/test_trace_config_batch_service.py
git commit -m "feat: handle trace config batch results"
```

---

### Task 5: Convert commands module to a package, then add CLI command group with `providers` and `template`

**Files:**
- Move: `api/commands.py` -> `api/commands/__init__.py`
- Create: `api/commands/trace_config.py`
- Create: `api/tests/unit_tests/commands/test_trace_config_command.py`

- [ ] **Step 1: Write failing CLI tests for providers and templates**

Create `api/tests/unit_tests/commands/test_trace_config_command.py`:

```python
import json

import yaml
from click.testing import CliRunner

from commands.trace_config import trace_config


def test_providers_command_lists_supported_providers():
    result = CliRunner().invoke(trace_config, ["providers"])

    assert result.exit_code == 0
    assert result.output.splitlines() == ["aliyun", "arize", "langfuse", "langsmith", "opik", "phoenix", "weave"]


def test_template_command_outputs_json_by_default():
    result = CliRunner().invoke(trace_config, ["template", "langfuse"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["provider"] == "langfuse"
    assert payload["tracing_config"]["public_key"] == "<required>"
    assert payload["tracing_config"]["host"] == "https://api.langfuse.com"


def test_template_command_outputs_yaml_when_requested():
    result = CliRunner().invoke(trace_config, ["template", "langfuse", "--format", "yaml"])

    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["provider"] == "langfuse"
    assert payload["tracing_config"]["secret_key"] == "<required>"


def test_template_all_outputs_each_provider():
    result = CliRunner().invoke(trace_config, ["template", "--all"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert sorted(payload.keys()) == ["aliyun", "arize", "langfuse", "langsmith", "opik", "phoenix", "weave"]
```

- [ ] **Step 2: Run tests to verify they fail because the command module is missing**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'commands.trace_config'`.

- [ ] **Step 3: Convert `api/commands.py` into a `commands` package without changing its public imports**

Run:

```bash
cd api
mkdir -p commands
git mv commands.py commands/__init__.py
```

Expected: `api/commands/__init__.py` contains the existing command definitions, and existing imports such as `from commands import reset_password` continue to resolve because `commands` is now a package.

- [ ] **Step 4: Implement `providers` and `template`**

Create `api/commands/trace_config.py`:

```python
from __future__ import annotations

import json
from typing import Any

import click
import yaml

from services.trace_config_batch_service import TraceConfigBatchError, TraceConfigBatchService


def _dump_payload(payload: Any, output_format: str) -> str:
    if output_format == "yaml":
        return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    return json.dumps(payload, indent=2, sort_keys=False)


@click.group("trace-config")
def trace_config():
    """Manage trace provider configuration in batch."""


@trace_config.command("providers")
def providers_command():
    """List supported trace providers."""
    for provider in TraceConfigBatchService.list_providers():
        click.echo(provider)


@trace_config.command("template")
@click.argument("provider", required=False)
@click.option("--all", "include_all", is_flag=True, help="Print templates for all supported providers.")
@click.option("--format", "output_format", type=click.Choice(["json", "yaml"]), default="json", show_default=True)
def template_command(provider: str | None, include_all: bool, output_format: str):
    """Print a trace provider input template."""
    try:
        if include_all:
            payload = TraceConfigBatchService.get_all_templates()
        elif provider:
            payload = TraceConfigBatchService.get_template(provider)
        else:
            raise click.UsageError("Provide a provider or use --all.")
    except TraceConfigBatchError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(_dump_payload(payload, output_format))
```

- [ ] **Step 5: Run CLI tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py -q
```

Expected: PASS for providers and template tests. - [ ] **Step 6: Commit**

```bash
git add api/commands/__init__.py api/commands/trace_config.py api/tests/unit_tests/commands/test_trace_config_command.py
git commit -m "feat: add trace config provider cli"
```

---

### Task 6: Add batch CLI parsing for inline JSON and config files

**Files:**
- Modify: `api/commands/trace_config.py`
- Modify: `api/tests/unit_tests/commands/test_trace_config_command.py`

- [ ] **Step 1: Add failing CLI batch tests**

Append tests:

```python
from unittest.mock import patch

from services.trace_config_batch_service import AppBatchResult, BatchResult


def successful_batch_result():
    return BatchResult(
        provider="langfuse",
        enabled=True,
        validation_skipped=False,
        results=[AppBatchResult(app_id="app-1", app_name="App One", status="created, enabled", enabled=True)],
    )


def test_batch_command_accepts_inline_json_config():
    with patch("commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()) as batch_upsert:
        result = CliRunner().invoke(
            trace_config,
            [
                "batch",
                "--provider",
                "langfuse",
                "--app-ids",
                "app-1,app-2",
                "--config-json",
                '{"public_key":"pk","secret_key":"sk"}',
            ],
        )

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1", "app-2"],
        tracing_config={"public_key": "pk", "secret_key": "sk"},
        enable=True,
        validate=True,
        fail_fast=False,
    )
    assert "Provider: langfuse" in result.output
    assert "app-1" in result.output
    assert "Summary: total=1 succeeded=1 failed=0" in result.output


def test_batch_command_accepts_yaml_file(tmp_path):
    config_file = tmp_path / "trace-config.yaml"
    config_file.write_text(
        "provider: langfuse\n"
        "app_ids:\n"
        "  - app-1\n"
        "tracing_config:\n"
        "  public_key: pk\n"
        "  secret_key: sk\n"
        "enable: false\n",
        encoding="utf-8",
    )

    with patch("commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()) as batch_upsert:
        result = CliRunner().invoke(trace_config, ["batch", "--file", str(config_file), "--skip-validate"])

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1"],
        tracing_config={"public_key": "pk", "secret_key": "sk"},
        enable=False,
        validate=False,
        fail_fast=False,
    )


def test_batch_command_exits_one_when_any_app_fails():
    failed_result = BatchResult(
        provider="langfuse",
        enabled=True,
        validation_skipped=False,
        results=[AppBatchResult(app_id="missing", status="failed", error="App not found")],
    )

    with patch("commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=failed_result):
        result = CliRunner().invoke(
            trace_config,
            [
                "batch",
                "--provider",
                "langfuse",
                "--app-ids",
                "missing",
                "--config-json",
                '{"public_key":"pk","secret_key":"sk"}',
            ],
        )

    assert result.exit_code == 1
    assert "failed: App not found" in result.output


def test_batch_command_rejects_invalid_json():
    result = CliRunner().invoke(
        trace_config,
        ["batch", "--provider", "langfuse", "--app-ids", "app-1", "--config-json", "not-json"],
    )

    assert result.exit_code != 0
    assert "Invalid JSON" in result.output
```

Patch paths should remain `commands.trace_config...` because Task 5 converts `commands` into a package.

- [ ] **Step 2: Run tests to verify failures**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py -q
```

Expected: FAIL because the `batch` command is not registered.

- [ ] **Step 3: Implement batch CLI helpers and command**

Add to the command module:

```python
from pathlib import Path


def _parse_app_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _load_config_file(path: str) -> dict[str, Any]:
    file_path = Path(path)
    try:
        raw_content = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise click.ClickException(f"Unable to read config file: {exc}") from exc

    try:
        if file_path.suffix.lower() == ".json":
            payload = json.loads(raw_content)
        else:
            payload = yaml.safe_load(raw_content)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise click.ClickException(f"Invalid JSON/YAML file: {exc}") from exc

    if not isinstance(payload, dict):
        raise click.ClickException("Config file must contain an object.")
    return payload


def _load_inline_json(value: str) -> dict[str, Any]:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise click.ClickException("--config-json must contain a JSON object.")
    return payload


def _print_batch_result(result: BatchResult):
    click.echo(f"Provider: {result.provider}")
    click.echo(f"Enable tracing: {str(result.enabled).lower()}")
    click.echo(f"External validation skipped: {str(result.validation_skipped).lower()}")
    for app_result in result.results:
        name = f" ({app_result.app_name})" if app_result.app_name else ""
        if app_result.error:
            click.echo(f"- {app_result.app_id}{name}: failed: {app_result.error}")
        else:
            click.echo(f"- {app_result.app_id}{name}: {app_result.status}")
    click.echo(f"Summary: total={result.total} succeeded={result.succeeded} failed={result.failed}")
```

Also import `BatchResult` for typing:

```python
from services.trace_config_batch_service import BatchResult, TraceConfigBatchError, TraceConfigBatchService
```

Add the command:

```python
@trace_config.command("batch")
@click.option("--provider", help="Trace provider name.")
@click.option("--app-ids", help="Comma-separated app IDs.")
@click.option("--config-json", help="Provider tracing_config as a JSON object.")
@click.option("--file", "config_file", help="JSON or YAML file containing provider, app_ids, tracing_config, and enable.")
@click.option("--no-enable", is_flag=True, help="Only upsert credentials/config; do not enable app tracing.")
@click.option("--skip-validate", is_flag=True, help="Skip external provider credential validation.")
@click.option("--fail-fast", is_flag=True, help="Stop after the first per-app failure.")
def batch_command(
    provider: str | None,
    app_ids: str | None,
    config_json: str | None,
    config_file: str | None,
    no_enable: bool,
    skip_validate: bool,
    fail_fast: bool,
):
    """Batch upsert trace provider configuration for explicit app IDs."""
    if config_file and (provider or app_ids or config_json):
        raise click.UsageError("Use either --file or inline --provider/--app-ids/--config-json input, not both.")

    if config_file:
        payload = _load_config_file(config_file)
        resolved_provider = payload.get("provider")
        resolved_app_ids = payload.get("app_ids") or []
        tracing_config = payload.get("tracing_config")
        enable = bool(payload.get("enable", True))
    else:
        if not provider or not app_ids or not config_json:
            raise click.UsageError("Inline mode requires --provider, --app-ids, and --config-json.")
        resolved_provider = provider
        resolved_app_ids = _parse_app_ids(app_ids)
        tracing_config = _load_inline_json(config_json)
        enable = True

    if no_enable:
        enable = False
    if not isinstance(resolved_provider, str) or not resolved_provider.strip():
        raise click.UsageError("Provider is required.")
    if not isinstance(resolved_app_ids, list) or not all(isinstance(app_id, str) for app_id in resolved_app_ids):
        raise click.UsageError("app_ids must be a list of strings.")
    if not isinstance(tracing_config, dict):
        raise click.UsageError("tracing_config must be an object.")

    try:
        result = TraceConfigBatchService.batch_upsert(
            provider=resolved_provider,
            app_ids=[app_id.strip() for app_id in resolved_app_ids if app_id.strip()],
            tracing_config=tracing_config,
            enable=enable,
            validate=not skip_validate,
            fail_fast=fail_fast,
        )
    except TraceConfigBatchError as exc:
        raise click.ClickException(str(exc)) from exc

    _print_batch_result(result)
    if result.has_failures:
        raise click.exceptions.Exit(1)
```

- [ ] **Step 4: Run CLI tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py -q
```

Expected: PASS for providers, template, and batch CLI tests.

- [ ] **Step 5: Commit**

```bash
git add api/commands/trace_config.py api/tests/unit_tests/commands/test_trace_config_command.py
git commit -m "feat: add trace config batch cli"
```

---

### Task 7: Add wizard CLI mode

**Files:**
- Modify: `api/commands/trace_config.py`
- Modify: `api/tests/unit_tests/commands/test_trace_config_command.py`

- [ ] **Step 1: Add failing wizard test**

Append:

```python

def test_wizard_collects_inputs_and_runs_batch():
    user_input = "\n".join(
        [
            "langfuse",
            "app-1,app-2",
            "pk",
            "sk",
            "",  # accept default host
            "y",  # enable tracing
            "n",  # skip external validation
            "y",  # final confirmation
            "",
        ]
    )

    with patch("commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()) as batch_upsert:
        result = CliRunner().invoke(trace_config, ["wizard"], input=user_input)

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1", "app-2"],
        tracing_config={"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        enable=True,
        validate=False,
        fail_fast=False,
    )
    assert "Supported providers:" in result.output
    assert "Provider: langfuse" in result.output
```

Patch path should remain `commands.trace_config.TraceConfigBatchService.batch_upsert`.

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py::test_wizard_collects_inputs_and_runs_batch -q
```

Expected: FAIL because `wizard` command is not registered.

- [ ] **Step 3: Implement wizard command**

Add helper:

```python
def _collect_config_from_template(provider: str) -> dict[str, Any]:
    template = TraceConfigBatchService.get_template(provider)
    config: dict[str, Any] = {}
    for key, default_value in template["tracing_config"].items():
        if default_value == "<required>":
            config[key] = click.prompt(key, hide_input="key" in key.lower() or "secret" in key.lower())
        else:
            prompted_value = click.prompt(f"{key}", default=str(default_value), show_default=True)
            config[key] = prompted_value
    return config
```

Add command:

```python
@trace_config.command("wizard")
def wizard_command():
    """Interactively configure trace provider settings for multiple apps."""
    providers = TraceConfigBatchService.list_providers()
    click.echo("Supported providers:")
    for provider_name in providers:
        click.echo(f"- {provider_name}")

    provider = click.prompt("Provider", type=click.Choice(providers))
    app_ids = _parse_app_ids(click.prompt("App IDs (comma-separated)"))
    tracing_config = _collect_config_from_template(provider)
    enable = click.confirm("Enable tracing after credentials are written?", default=True)
    validate = click.confirm("Validate credentials with provider API?", default=True)

    click.echo("Summary:")
    click.echo(f"Provider: {provider}")
    click.echo(f"App IDs: {', '.join(app_ids)}")
    click.echo(f"Enable tracing: {str(enable).lower()}")
    click.echo(f"External validation: {str(validate).lower()}")
    if not click.confirm("Proceed?", default=False):
        click.echo("Aborted.")
        return

    try:
        result = TraceConfigBatchService.batch_upsert(
            provider=provider,
            app_ids=app_ids,
            tracing_config=tracing_config,
            enable=enable,
            validate=validate,
            fail_fast=False,
        )
    except TraceConfigBatchError as exc:
        raise click.ClickException(str(exc)) from exc

    _print_batch_result(result)
    if result.has_failures:
        raise click.exceptions.Exit(1)
```

- [ ] **Step 4: Run wizard and full CLI tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/commands/trace_config.py api/tests/unit_tests/commands/test_trace_config_command.py
git commit -m "feat: add trace config wizard cli"
```

---

### Task 8: Register the Flask command group

**Files:**
- Modify: `api/extensions/ext_commands.py`
- Modify: `api/tests/unit_tests/commands/test_trace_config_command.py`

- [ ] **Step 1: Add failing registration test**

Append:

```python
from flask import Flask

from extensions.ext_commands import init_app


def test_trace_config_command_is_registered_on_flask_app():
    flask_app = Flask(__name__)

    init_app(flask_app)

    assert "trace-config" in flask_app.cli.commands
```

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py::test_trace_config_command_is_registered_on_flask_app -q
```

Expected: FAIL because `trace-config` is not in `flask_app.cli.commands`.

- [ ] **Step 3: Register command in `ext_commands.py`**

Modify `api/extensions/ext_commands.py` to import `trace_config` from the command module.

Import the new command group:

```python
    from commands.trace_config import trace_config
```

Add `trace_config` to `cmds_to_register`:

```python
        trace_config,
```

- [ ] **Step 4: Run registration test**

Run:

```bash
cd api && uv run pytest tests/unit_tests/commands/test_trace_config_command.py::test_trace_config_command_is_registered_on_flask_app -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/extensions/ext_commands.py api/tests/unit_tests/commands/test_trace_config_command.py
git commit -m "feat: register trace config flask command"
```

---

### Task 9: Harden service transaction semantics and validation ordering

**Files:**
- Modify: `api/services/trace_config_batch_service.py`
- Modify: `api/tests/unit_tests/services/test_trace_config_batch_service.py`

- [ ] **Step 1: Add tests that batch-level validation happens before app writes**

Append:

```python

def test_batch_level_schema_failure_happens_before_app_writes(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with pytest.raises(TraceConfigBatchError, match="Invalid tracing config"):
        TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1"],
            tracing_config={"public_key": "pk-only"},
        )

    assert fake_session.commits == 0
    assert fake_session.added == []


def test_external_validation_is_called_once_for_multiple_apps(monkeypatch):
    fake_session = FakeSession()
    fake_session.apps["app-1"] = SimpleNamespace(id="app-1", tenant_id="tenant-1", name="App One", tracing=None)
    fake_session.apps["app-2"] = SimpleNamespace(id="app-2", tenant_id="tenant-1", name="App Two", tracing=None)
    install_fake_lookup(monkeypatch, fake_session)

    with patch("services.trace_config_batch_service.OpsTraceManager.check_trace_config_is_effective", return_value=True) as api_check, patch(
        "services.trace_config_batch_service.OpsTraceManager.encrypt_tracing_config",
        return_value={"public_key": "encrypted-pk", "secret_key": "encrypted-sk", "host": "https://api.langfuse.com"},
    ):
        result = TraceConfigBatchService.batch_upsert(
            provider="langfuse",
            app_ids=["app-1", "app-2"],
            tracing_config={"public_key": "pk", "secret_key": "sk"},
            enable=False,
        )

    assert result.succeeded == 2
    api_check.assert_called_once()
```

- [ ] **Step 2: Run service tests**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: PASS if Task 2 and Task 3 ordered validation correctly; otherwise fix ordering.

- [ ] **Step 3: Refine implementation only if needed**

Ensure `batch_upsert()` always calls `validate_credentials()` before entering the `for app_id in app_ids` loop and never calls `OpsTraceManager.check_trace_config_is_effective()` from `_upsert_one_app()`.

- [ ] **Step 4: Run service tests again**

Run:

```bash
cd api && uv run pytest tests/unit_tests/services/test_trace_config_batch_service.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/services/trace_config_batch_service.py api/tests/unit_tests/services/test_trace_config_batch_service.py
git commit -m "test: cover trace config batch validation ordering"
```

---

### Task 10: Run targeted verification and static checks

**Files:**
- No source files expected unless verification reveals issues.

- [ ] **Step 1: Run all new unit tests**

Run:

```bash
cd api && uv run pytest \
  tests/unit_tests/services/test_trace_config_batch_service.py \
  tests/unit_tests/commands/test_trace_config_command.py \
  -q
```

Expected: all tests PASS.

- [ ] **Step 2: Run existing ops tests likely affected by imports/provider metadata**

Run:

```bash
cd api && uv run pytest tests/unit_tests/core/ops/test_config_entity.py -q
```

Expected: PASS.

- [ ] **Step 3: Run Ruff on changed files if available**

Run:

```bash
cd api && uv run ruff check services/trace_config_batch_service.py commands/__init__.py commands/trace_config.py extensions/ext_commands.py tests/unit_tests/services/test_trace_config_batch_service.py tests/unit_tests/commands/test_trace_config_command.py
```

Expected: PASS.

- [ ] **Step 4: Run formatting check if available**

Run:

```bash
cd api && uv run ruff format --check services/trace_config_batch_service.py commands/__init__.py commands/trace_config.py extensions/ext_commands.py tests/unit_tests/services/test_trace_config_batch_service.py tests/unit_tests/commands/test_trace_config_command.py
```

Expected: PASS.

- [ ] **Step 5: Commit verification fixes if any**

If verification required code or test changes:

```bash
git add api/services/trace_config_batch_service.py api/commands/__init__.py api/commands/trace_config.py api/extensions/ext_commands.py api/tests/unit_tests/services/test_trace_config_batch_service.py api/tests/unit_tests/commands/test_trace_config_command.py
git commit -m "fix: polish trace config batch command"
```

Only add files that were changed.

---

### Task 11: Enhance wizard with workspace/app selection plus manual app ID fallback

**Files:**
- Modify: `api/services/trace_config_batch_service.py`
- Modify: `api/commands/trace_config.py`
- Modify: `api/tests/unit_tests/services/test_trace_config_batch_service.py`
- Modify: `api/tests/unit_tests/commands/test_trace_config_command.py`

- [ ] **Step 1: Add service tests for read-only wizard lookup helpers**

Add tests that verify:

```python
def test_list_workspaces_returns_numbered_workspace_options(monkeypatch):
    # Mock db.session.query(Tenant).order_by(...).all() to return two tenants.
    # Assert TraceConfigBatchService.list_workspaces() returns stable id/name dicts.


def test_list_apps_for_workspace_returns_numbered_app_options(monkeypatch):
    # Mock db.session.query(App).filter(App.tenant_id == tenant_id).order_by(...).all().
    # Assert app id, name, and mode are exposed without credentials.
```

Expected behavior:
- Workspace options include `id` and `name`.
- App options include `id`, `name`, and `mode`.
- Empty workspaces return an empty list rather than failing.

- [ ] **Step 2: Implement service lookup helpers**

Add small read-only dataclasses or dictionaries in `TraceConfigBatchService`:

```python
@classmethod
def list_workspaces(cls) -> list[dict[str, str]]:
    tenants = db.session.query(Tenant).order_by(Tenant.created_at.asc(), Tenant.id.asc()).all()
    return [{"id": tenant.id, "name": tenant.name or tenant.id} for tenant in tenants]

@classmethod
def list_apps_for_workspace(cls, tenant_id: str) -> list[dict[str, str]]:
    apps = (
        db.session.query(App)
        .filter(App.tenant_id == tenant_id)
        .order_by(App.created_at.asc(), App.id.asc())
        .all()
    )
    return [
        {"id": app.id, "name": app.name or app.id, "mode": str(app.mode)}
        for app in apps
    ]
```

Import `Tenant` from the existing models module used by the repository. Keep these helpers separate from `batch_upsert()` so CI mode remains explicit-app-ID only.

- [ ] **Step 3: Add CLI tests for both wizard app selection modes**

Update the existing manual wizard test to select manual mode first. Add a workspace selection test:

```python
def test_wizard_selects_apps_from_workspace():
    user_input = "\n".join([
        "langfuse",
        "1",  # Select from workspace
        "1",  # Workspace number
        "1,2",  # App numbers
        "pk",
        "sk",
        "",
        "y",
        "n",
        "y",
        "",
    ])

    with patch("commands.trace_config.TraceConfigBatchService.list_workspaces", return_value=[{"id": "tenant-1", "name": "Workspace One"}]), patch(
        "commands.trace_config.TraceConfigBatchService.list_apps_for_workspace",
        return_value=[
            {"id": "app-1", "name": "App One", "mode": "chat"},
            {"id": "app-2", "name": "App Two", "mode": "workflow"},
        ],
    ), patch("commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()) as batch_upsert:
        result = CliRunner().invoke(trace_config, ["wizard"], input=user_input)

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1", "app-2"],
        tracing_config={"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        enable=True,
        validate=False,
        fail_fast=False,
    )
```

Also add invalid-number tests for:
- Workspace number outside the displayed range.
- App number outside the displayed range.

- [ ] **Step 4: Implement wizard selection prompts**

Add helpers in `api/commands/trace_config.py`:

```python
def _choose_numbered_option(options: list[dict[str, str]], prompt: str) -> dict[str, str]:
    raw_value = click.prompt(prompt)
    try:
        index = int(raw_value) - 1
    except ValueError as exc:
        raise click.ClickException(f"Invalid selection: {raw_value}") from exc
    if index < 0 or index >= len(options):
        raise click.ClickException(f"Invalid selection: {raw_value}")
    return options[index]


def _choose_numbered_options(options: list[dict[str, str]], prompt: str) -> list[dict[str, str]]:
    raw_value = click.prompt(prompt)
    selected = []
    for part in raw_value.split(","):
        try:
            index = int(part.strip()) - 1
        except ValueError as exc:
            raise click.ClickException(f"Invalid selection: {part.strip()}") from exc
        if index < 0 or index >= len(options):
            raise click.ClickException(f"Invalid selection: {part.strip()}")
        selected.append(options[index])
    return selected
```

Add `_collect_app_ids_for_wizard()`:

```python
def _collect_app_ids_for_wizard() -> list[str]:
    click.echo("App selection mode:")
    click.echo("1. Select from workspace")
    click.echo("2. Enter app IDs manually")
    mode = click.prompt("Mode", type=click.Choice(["1", "2"]))
    if mode == "2":
        return _parse_app_ids(click.prompt("App IDs (comma-separated)"))

    workspaces = TraceConfigBatchService.list_workspaces()
    if not workspaces:
        raise click.ClickException("No workspaces found.")
    for idx, workspace in enumerate(workspaces, start=1):
        click.echo(f"{idx}. {workspace['name']} ({workspace['id']})")
    workspace = _choose_numbered_option(workspaces, "Workspace")

    apps = TraceConfigBatchService.list_apps_for_workspace(workspace["id"])
    if not apps:
        raise click.ClickException(f"No apps found in workspace {workspace['name']}.")
    for idx, app in enumerate(apps, start=1):
        click.echo(f"{idx}. {app['name']} [{app['mode']}] ({app['id']})")
    selected_apps = _choose_numbered_options(apps, "Apps")
    return [app["id"] for app in selected_apps]
```

Update `wizard_command()` to call `_collect_app_ids_for_wizard()` instead of directly prompting for comma-separated app IDs.

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd api && uv run pytest \
  tests/unit_tests/services/test_trace_config_batch_service.py \
  tests/unit_tests/commands/test_trace_config_command.py \
  -q
```

Expected: PASS.

- [ ] **Step 6: Run lint/format checks**

Run:

```bash
cd api && uv run ruff check services/trace_config_batch_service.py commands/trace_config.py tests/unit_tests/services/test_trace_config_batch_service.py tests/unit_tests/commands/test_trace_config_command.py
cd api && uv run ruff format --check services/trace_config_batch_service.py commands/trace_config.py tests/unit_tests/services/test_trace_config_batch_service.py tests/unit_tests/commands/test_trace_config_command.py
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add api/services/trace_config_batch_service.py api/commands/trace_config.py api/tests/unit_tests/services/test_trace_config_batch_service.py api/tests/unit_tests/commands/test_trace_config_command.py
git commit -m "feat: add workspace app picker to trace config wizard"
```

---

## Self-Review

- **Spec coverage:** Covered provider list, dynamic templates, inline/file batch input, schema validation, single external validation per batch, encrypted per-app upsert, default enable, `--no-enable`, `--skip-validate`, `--fail-fast`, best-effort per-app failures, deterministic non-secret output, wizard manual app ID entry, wizard workspace/app picker, command registration, and tests.
- **Placeholder scan:** No implementation step depends on unresolved placeholder behavior. The plan explicitly converts the current `api/commands.py` module into `api/commands/__init__.py` so `api/commands/trace_config.py` can exist without breaking `from commands import ...` imports.
- **Type consistency:** `AppBatchResult`, `BatchResult`, `TraceConfigBatchError`, and `TraceConfigBatchService` names are consistent across service, CLI, and tests.
- **Risk note:** The existing `OpsTraceManager.update_app_tracing_config()` commits internally. If implementation finds that double-commit complicates transaction isolation, replace that call inside `_upsert_one_app()` with a local `app.tracing = json.dumps({"enabled": True, "tracing_provider": provider})` write and one final `db.session.commit()`, then update tests to assert the local write instead of the manager call.
