"""Coordinate nested-workflow provider parent contexts through Redis.

The coordinator owns storage, compatibility decisions, validation, and retry
signals. Provider adapters only create and consume their opaque context fields.
"""

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, ValidationError

from core.helper.trace_id_helper import ParentTraceContext
from core.ops.exceptions import (
    InvalidTraceParentContextError,
    PendingTraceParentContextError,
    TraceParentContextAccessError,
)

_PARENT_CONTEXT_TTL_SECONDS = 300
_PARENT_CONTEXT_KEY_PREFIX = "trace:unified:parent:"


class RedisParentContextStore(Protocol):
    def setex(self, name: str, time: int, value: str) -> object:
        raise NotImplementedError

    def get(self, name: str) -> bytes | str | None:
        raise NotImplementedError


class ProviderParentContext(BaseModel):
    """Versioned envelope containing the minimum provider restoration state."""

    version: Literal[1] = 1
    provider: str
    scope: str
    trace_id: str
    parent_id: str
    provider_context: dict[str, str]

    model_config = ConfigDict(extra="forbid", frozen=True)


@dataclass(frozen=True)
class ParentDestination:
    provider: str
    scope: str
    unified: bool


class ParentResolutionKind(StrEnum):
    RESTORED = "restored"
    LINKED_ROOT = "linked_root"


@dataclass(frozen=True)
class ParentResolution:
    kind: ParentResolutionKind
    context: ProviderParentContext | None = None
    linked_parent: ParentTraceContext | None = None

    @classmethod
    def restored(cls, context: ProviderParentContext) -> "ParentResolution":
        return cls(kind=ParentResolutionKind.RESTORED, context=context)

    @classmethod
    def linked_root(cls, parent: ParentTraceContext) -> "ParentResolution":
        return cls(kind=ParentResolutionKind.LINKED_ROOT, linked_parent=parent)


ParentDestinationResolver = Callable[[str], ParentDestination | None]


def destination_scope(provider: str, endpoint: str, project: str) -> str:
    """Return a stable non-secret fingerprint for a provider destination."""
    value = f"{provider}\0{endpoint.rstrip('/')}\0{project}"
    return hashlib.sha256(value.encode()).hexdigest()


class ParentContextCoordinator:
    """Publish and resolve cross-task parent contexts for unified providers."""

    def __init__(
        self,
        store: RedisParentContextStore,
        resolve_parent_destination: ParentDestinationResolver,
    ) -> None:
        self._store = store
        self._resolve_parent_destination = resolve_parent_destination

    @staticmethod
    def _key(parent_node_execution_id: str) -> str:
        return f"{_PARENT_CONTEXT_KEY_PREFIX}{parent_node_execution_id}"

    def publish(self, parent_node_execution_id: str, context: ProviderParentContext) -> None:
        """Persist an accepted provider parent so a nested task can restore it."""
        try:
            self._store.setex(
                self._key(parent_node_execution_id),
                _PARENT_CONTEXT_TTL_SECONDS,
                context.model_dump_json(),
            )
        except Exception as error:
            raise TraceParentContextAccessError(
                f"Could not publish unified parent context for node_execution_id={parent_node_execution_id}"
            ) from error

    def resolve(
        self,
        parent: ParentTraceContext,
        *,
        expected_provider: str,
        expected_scope: str,
    ) -> ParentResolution:
        """Restore compatible context or explicitly select a linked new root."""
        destination = self._resolve_parent_destination(parent.parent_workflow_run_id)
        if (
            destination is None
            or not destination.unified
            or destination.provider != expected_provider
            or destination.scope != expected_scope
        ):
            return ParentResolution.linked_root(parent)

        parent_node_execution_id = parent.parent_node_execution_id
        if not parent_node_execution_id:
            raise InvalidTraceParentContextError("Nested workflow parent context has no node execution ID")

        try:
            raw_context = self._store.get(self._key(parent_node_execution_id))
        except Exception as error:
            raise TraceParentContextAccessError(
                f"Could not read unified parent context for node_execution_id={parent_node_execution_id}"
            ) from error

        if raw_context is None:
            raise PendingTraceParentContextError(parent_node_execution_id)

        try:
            context = ProviderParentContext.model_validate_json(raw_context)
        except (ValidationError, ValueError, TypeError) as error:
            raise InvalidTraceParentContextError(
                f"Invalid unified parent context for node_execution_id={parent_node_execution_id}"
            ) from error

        if context.provider != expected_provider or context.scope != expected_scope:
            raise InvalidTraceParentContextError(
                "Stored unified parent context does not match the expected provider destination: "
                f"node_execution_id={parent_node_execution_id}"
            )
        return ParentResolution.restored(context)
