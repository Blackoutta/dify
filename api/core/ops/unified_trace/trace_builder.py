"""Build provider-neutral traces from core ops trace entities."""

from core.ops.entities.trace_entity import MessageTraceInfo, WorkflowTraceInfo


def resolve_session_id(trace_info: WorkflowTraceInfo | MessageTraceInfo) -> str:
    """Resolve an explicit trace session before stable Dify fallbacks."""
    custom_session_id = trace_info.metadata.get("trace_session_id")
    if isinstance(custom_session_id, str) and custom_session_id:
        return custom_session_id

    if isinstance(trace_info, WorkflowTraceInfo):
        if trace_info.conversation_id:
            return trace_info.conversation_id
        parent_workflow_run_id, _ = trace_info.resolved_parent_context
        return parent_workflow_run_id or trace_info.workflow_run_id

    if trace_info.message_data is None:
        return ""
    conversation_id = getattr(trace_info.message_data, "conversation_id", None)
    return conversation_id if isinstance(conversation_id, str) else ""
