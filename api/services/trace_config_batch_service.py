from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError
from pydantic_core import PydanticUndefined

from core.ops.entities.config_entity import TracingProviderEnum
from core.ops.ops_trace_manager import OpsTraceManager, provider_config_map
from extensions.ext_database import db
from models import Tenant
from models.model import App, TraceAppConfig


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
            if model_field.is_required() or model_field.default is PydanticUndefined:
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

    @classmethod
    def list_workspaces(cls) -> list[dict[str, str]]:
        tenants = db.session.query(Tenant).order_by(Tenant.created_at.asc(), Tenant.id.asc()).all()
        return [{"id": tenant.id, "name": tenant.name or tenant.id} for tenant in tenants]

    @classmethod
    def list_apps_for_workspace(cls, tenant_id: str) -> list[dict[str, str]]:
        apps = (
            db.session.query(App).filter(App.tenant_id == tenant_id).order_by(App.created_at.asc(), App.id.asc()).all()
        )
        return [
            {
                "id": app.id,
                "name": app.name or app.id,
                "mode": str(getattr(app.mode, "value", app.mode)),
            }
            for app in apps
        ]

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
    def _upsert_one_app(cls, app_id: str, provider: str, tracing_config: dict[str, Any], *, enable: bool):
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
        cleaned_app_ids = [app_id.strip() for app_id in app_ids if app_id.strip()]
        if not cleaned_app_ids:
            raise TraceConfigBatchError("At least one app ID is required")

        validated_config = cls.validate_credentials(normalized_provider, tracing_config, validate=validate)
        batch_result = BatchResult(provider=normalized_provider, enabled=enable, validation_skipped=not validate)

        for app_id in cleaned_app_ids:
            try:
                batch_result.results.append(
                    cls._upsert_one_app(app_id, normalized_provider, validated_config, enable=enable)
                )
            except Exception as exc:
                db.session.rollback()
                batch_result.results.append(AppBatchResult(app_id=app_id, status="failed", error=str(exc)))
                if fail_fast:
                    break

        return batch_result
