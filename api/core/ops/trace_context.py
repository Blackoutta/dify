from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, StrictStr, ValidationError

MAX_TRACE_SESSION_ID_LENGTH = 512


class ParentTraceContext(BaseModel):
    """Private trace context propagated from an outer workflow tool node."""

    parent_workflow_run_id: StrictStr
    parent_node_execution_id: StrictStr

    model_config = ConfigDict(extra="forbid")


def parent_trace_context_from_metadata(metadata: Mapping[str, Any]) -> ParentTraceContext | None:
    raw_context = metadata.get("parent_trace_context")
    if isinstance(raw_context, ParentTraceContext):
        return raw_context
    if isinstance(raw_context, Mapping):
        try:
            return ParentTraceContext.model_validate(raw_context)
        except ValidationError:
            return None
    return None


def extract_parent_trace_context_from_args(args: Mapping[str, Any]) -> dict[str, ParentTraceContext]:
    raw_context = args.get("parent_trace_context")
    if isinstance(raw_context, ParentTraceContext):
        return {"parent_trace_context": raw_context}
    if isinstance(raw_context, Mapping):
        try:
            return {"parent_trace_context": ParentTraceContext.model_validate(raw_context)}
        except ValidationError:
            return {}
    return {}


def normalize_trace_session_id(raw_value: Any) -> str | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise ValueError("trace_session_id must be a string")
    value = raw_value.strip()
    if not value:
        return None
    if len(value) > MAX_TRACE_SESSION_ID_LENGTH:
        raise ValueError("trace_session_id must be 512 characters or fewer")
    return value


def extract_trace_session_id_from_args(args: Mapping[str, Any]) -> dict[str, str]:
    trace_session_id = normalize_trace_session_id(args.get("trace_session_id"))
    return {"trace_session_id": trace_session_id} if trace_session_id else {}
