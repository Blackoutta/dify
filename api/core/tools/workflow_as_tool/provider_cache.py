from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from json import JSONDecodeError
from typing import Any

from configs import dify_config
from extensions.ext_redis import redis_client
from models.account import Account
from models.model import App
from models.tools import WorkflowToolProvider
from models.workflow import Workflow

CACHE_SCHEMA_VERSION = 1
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WorkflowToolProviderCacheMetadata:
    provider: WorkflowToolProvider
    app: App
    workflow: Workflow
    user: Account | None


def workflow_tool_provider_cache_key(tenant_id: str, provider_id: str) -> str:
    return f"workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:v1"


def workflow_tool_provider_lock_key(tenant_id: str, provider_id: str) -> str:
    return f"workflow_tool_provider:tenant:{tenant_id}:provider:{provider_id}:lock"


def _datetime_to_payload(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _datetime_from_payload(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def build_workflow_tool_provider_cache_payload(
    *,
    provider: WorkflowToolProvider,
    app: App,
    workflow: Workflow,
    user: Account | None,
) -> dict[str, Any]:
    return {
        "schema_version": CACHE_SCHEMA_VERSION,
        "provider": {
            "id": provider.id,
            "tenant_id": provider.tenant_id,
            "app_id": provider.app_id,
            "user_id": provider.user_id,
            "name": provider.name,
            "label": provider.label,
            "description": provider.description,
            "icon": provider.icon,
            "version": provider.version,
            "parameter_configuration": provider.parameter_configuration,
            "privacy_policy": provider.privacy_policy,
        },
        "app": {
            "id": app.id,
            "tenant_id": app.tenant_id,
            "name": app.name,
            "description": app.description,
            "mode": app.mode,
            "icon_type": app.icon_type,
            "icon": app.icon,
            "icon_background": app.icon_background,
            "app_model_config_id": app.app_model_config_id,
            "workflow_id": app.workflow_id,
            "status": app.status,
            "enable_site": app.enable_site,
            "enable_api": app.enable_api,
            "api_rpm": app.api_rpm,
            "api_rph": app.api_rph,
            "is_demo": app.is_demo,
            "is_public": app.is_public,
            "is_universal": app.is_universal,
            "tracing": app.tracing,
            "max_active_requests": app.max_active_requests,
            "created_by": app.created_by,
            "updated_by": app.updated_by,
            "use_icon_as_answer_icon": app.use_icon_as_answer_icon,
        },
        "workflow": {
            "id": workflow.id,
            "tenant_id": workflow.tenant_id,
            "app_id": workflow.app_id,
            "type": workflow.type,
            "version": workflow.version,
            "marked_name": workflow.marked_name,
            "marked_comment": workflow.marked_comment,
            "graph": workflow.graph,
            "features": workflow.features,
            "created_by": workflow.created_by,
            "created_at": _datetime_to_payload(workflow.created_at),
            "updated_by": workflow.updated_by,
            "updated_at": _datetime_to_payload(workflow.updated_at),
            "environment_variables": workflow._environment_variables,
            "conversation_variables": workflow._conversation_variables,
        },
        "user": None if user is None else {"id": user.id, "name": user.name},
    }


def models_from_workflow_tool_provider_cache_payload(
    payload: dict[str, Any],
) -> WorkflowToolProviderCacheMetadata | None:
    if payload.get("schema_version") != CACHE_SCHEMA_VERSION:
        return None

    provider_payload = payload["provider"]
    app_payload = payload["app"]
    workflow_payload = payload["workflow"]
    user_payload = payload.get("user")

    provider = WorkflowToolProvider(
        id=provider_payload["id"],
        tenant_id=provider_payload["tenant_id"],
        app_id=provider_payload["app_id"],
        user_id=provider_payload["user_id"],
        name=provider_payload["name"],
        label=provider_payload["label"],
        description=provider_payload["description"],
        icon=provider_payload["icon"],
        version=provider_payload["version"],
        parameter_configuration=provider_payload["parameter_configuration"],
        privacy_policy=provider_payload.get("privacy_policy") or "",
    )

    app = App(**app_payload)

    workflow = Workflow(
        id=workflow_payload["id"],
        tenant_id=workflow_payload["tenant_id"],
        app_id=workflow_payload["app_id"],
        type=workflow_payload["type"],
        version=workflow_payload["version"],
        marked_name=workflow_payload.get("marked_name") or "",
        marked_comment=workflow_payload.get("marked_comment") or "",
        graph=workflow_payload["graph"],
        features=workflow_payload["features"],
        created_by=workflow_payload["created_by"],
        created_at=_datetime_from_payload(workflow_payload.get("created_at")),
        updated_by=workflow_payload.get("updated_by"),
        updated_at=_datetime_from_payload(workflow_payload.get("updated_at")),
    )
    workflow._environment_variables = workflow_payload.get("environment_variables") or "{}"
    workflow._conversation_variables = workflow_payload.get("conversation_variables") or "{}"

    user = None if user_payload is None else Account(id=user_payload["id"], name=user_payload["name"])
    return WorkflowToolProviderCacheMetadata(provider=provider, app=app, workflow=workflow, user=user)


def _cache_enabled() -> bool:
    return dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_TTL > 0


def _decode_redis_value(value: bytes | str) -> dict[str, Any]:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise ValueError("workflow tool provider cache payload must be a JSON object")
    return decoded


def get_cached_workflow_tool_provider_metadata(
    tenant_id: str,
    provider_id: str,
) -> WorkflowToolProviderCacheMetadata | None:
    if not _cache_enabled():
        return None

    key = workflow_tool_provider_cache_key(tenant_id, provider_id)
    try:
        cached_value = redis_client.get(key)
    except Exception:
        logger.warning("Failed to get workflow tool provider metadata cache", exc_info=True)
        return None

    if not cached_value:
        return None

    try:
        payload = _decode_redis_value(cached_value)
        metadata = models_from_workflow_tool_provider_cache_payload(payload)
    except (JSONDecodeError, UnicodeDecodeError, KeyError, TypeError, ValueError):
        logger.warning("Invalid workflow tool provider metadata cache payload", exc_info=True)
        invalidate_workflow_tool_provider_cache(tenant_id, provider_id)
        return None

    if metadata is None:
        invalidate_workflow_tool_provider_cache(tenant_id, provider_id)
        return None

    return metadata


def set_cached_workflow_tool_provider_metadata(
    tenant_id: str,
    provider_id: str,
    payload: dict[str, Any],
) -> None:
    if not _cache_enabled():
        return

    key = workflow_tool_provider_cache_key(tenant_id, provider_id)
    try:
        redis_client.setex(key, dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_TTL, json.dumps(payload))
    except Exception:
        logger.warning("Failed to set workflow tool provider metadata cache", exc_info=True)


def invalidate_workflow_tool_provider_cache(tenant_id: str, provider_id: str) -> None:
    key = workflow_tool_provider_cache_key(tenant_id, provider_id)
    try:
        redis_client.delete(key)
    except Exception:
        logger.warning("Failed to delete workflow tool provider metadata cache", exc_info=True)


def _payload_from_metadata(metadata: WorkflowToolProviderCacheMetadata) -> dict[str, Any]:
    return build_workflow_tool_provider_cache_payload(
        provider=metadata.provider,
        app=metadata.app,
        workflow=metadata.workflow,
        user=metadata.user,
    )


def _try_acquire_lock(tenant_id: str, provider_id: str):
    lock_name = workflow_tool_provider_lock_key(tenant_id, provider_id)
    try:
        lock = redis_client.lock(lock_name, timeout=dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_LOCK_TIMEOUT)
        if lock.acquire(blocking=False):
            return lock
    except Exception:
        logger.warning("Failed to acquire workflow tool provider metadata cache lock", exc_info=True)
    return None


def _release_lock(lock) -> None:
    try:
        lock.release()
    except Exception:
        logger.warning("Failed to release workflow tool provider metadata cache lock", exc_info=True)


def _wait_for_cached_metadata(tenant_id: str, provider_id: str) -> WorkflowToolProviderCacheMetadata | None:
    deadline = time.monotonic() + dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_TIMEOUT
    while time.monotonic() < deadline:
        time.sleep(dify_config.WORKFLOW_TOOL_PROVIDER_CACHE_WAIT_INTERVAL)
        metadata = get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
        if metadata is not None:
            return metadata
    return None


def get_or_load_workflow_tool_provider_metadata(
    tenant_id: str,
    provider_id: str,
    loader: Callable[[], WorkflowToolProviderCacheMetadata],
) -> WorkflowToolProviderCacheMetadata:
    if not _cache_enabled():
        return loader()

    cached_metadata = get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
    if cached_metadata is not None:
        return cached_metadata

    lock = _try_acquire_lock(tenant_id, provider_id)
    if lock is not None:
        try:
            cached_metadata = get_cached_workflow_tool_provider_metadata(tenant_id, provider_id)
            if cached_metadata is not None:
                return cached_metadata
            metadata = loader()
            set_cached_workflow_tool_provider_metadata(tenant_id, provider_id, _payload_from_metadata(metadata))
            return metadata
        finally:
            _release_lock(lock)

    waited_metadata = _wait_for_cached_metadata(tenant_id, provider_id)
    if waited_metadata is not None:
        return waited_metadata

    metadata = loader()
    set_cached_workflow_tool_provider_metadata(tenant_id, provider_id, _payload_from_metadata(metadata))
    return metadata
