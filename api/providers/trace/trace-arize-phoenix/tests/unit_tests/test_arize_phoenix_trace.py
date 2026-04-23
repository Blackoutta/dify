from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from dify_trace_arize_phoenix.arize_phoenix_trace import (
    _NODE_TYPE_TO_SPAN_KIND,
    _get_node_span_kind,
    _resolve_workflow_parent_context,
    _resolve_workflow_session_id,
)
from openinference.semconv.trace import OpenInferenceSpanKindValues

from core.ops.entities.trace_entity import WorkflowTraceInfo
from graphon.enums import BUILT_IN_NODE_TYPES, BuiltinNodeTypes


def _dt() -> datetime:
    return datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)


def _make_workflow_info(**kwargs) -> WorkflowTraceInfo:
    defaults = {
        "workflow_id": "workflow-1",
        "tenant_id": "tenant-1",
        "workflow_run_id": "workflow-run-1",
        "workflow_run_elapsed_time": 1.0,
        "workflow_run_status": "succeeded",
        "workflow_run_inputs": {"input": "value"},
        "workflow_run_outputs": {"output": "value"},
        "workflow_run_version": "1.0",
        "total_tokens": 10,
        "file_list": ["file-1"],
        "query": "hello",
        "metadata": {"app_id": "app-1"},
        "start_time": _dt(),
        "end_time": _dt() + timedelta(seconds=1),
    }
    defaults.update(kwargs)
    return WorkflowTraceInfo(**defaults)


class TestGetNodeSpanKind:
    """Tests for _get_node_span_kind helper."""

    def test_all_node_types_are_mapped_correctly(self):
        """Ensure every built-in node type is mapped to the correct span kind."""
        # Mappings for node types that have a specialised span kind.
        special_mappings = {
            BuiltinNodeTypes.LLM: OpenInferenceSpanKindValues.LLM,
            BuiltinNodeTypes.KNOWLEDGE_RETRIEVAL: OpenInferenceSpanKindValues.RETRIEVER,
            BuiltinNodeTypes.TOOL: OpenInferenceSpanKindValues.TOOL,
            BuiltinNodeTypes.AGENT: OpenInferenceSpanKindValues.AGENT,
        }

        # Test that every built-in node type is mapped to the correct span kind.
        # Node types not in `special_mappings` should default to CHAIN.
        for node_type in BUILT_IN_NODE_TYPES:
            expected_span_kind = special_mappings.get(node_type, OpenInferenceSpanKindValues.CHAIN)
            actual_span_kind = _get_node_span_kind(node_type)
            assert actual_span_kind == expected_span_kind, (
                f"Node type {node_type!r} was mapped to {actual_span_kind}, but {expected_span_kind} was expected."
            )

    def test_unknown_string_defaults_to_chain(self):
        """An unrecognised node type string should still return CHAIN."""
        assert _get_node_span_kind("some-future-node-type") == OpenInferenceSpanKindValues.CHAIN

    def test_stale_dataset_retrieval_not_in_mapping(self):
        """The old 'dataset_retrieval' string was never a valid NodeType value;
        make sure it is not present in the mapping dictionary."""
        assert "dataset_retrieval" not in _NODE_TYPE_TO_SPAN_KIND


class TestWorkflowSessionResolution:
    def test_prefers_conversation_id(self):
        info = _make_workflow_info(conversation_id="conversation-1")

        assert _resolve_workflow_session_id(info) == "conversation-1"

    def test_prefers_nested_parent_session_id(self):
        info = _make_workflow_info(
            conversation_id=None,
            metadata={
                "app_id": "app-1",
                "parent_trace_context": {
                    "session_id": "parent-session-1",
                },
            },
        )

        assert _resolve_workflow_session_id(info) == "parent-session-1"

    def test_ignores_nested_parent_conversation_id(self):
        info = _make_workflow_info(
            conversation_id=None,
            metadata={
                "app_id": "app-1",
                "parent_trace_context": {
                    "conversation_id": "parent-conversation-1",
                },
            },
        )

        assert _resolve_workflow_session_id(info) == "workflow-run-1"

    def test_falls_back_to_workflow_run_id(self):
        info = _make_workflow_info(conversation_id=None)

        assert _resolve_workflow_session_id(info) == "workflow-run-1"

    def test_parent_context_helper_delegates_to_resolved_parent_context(self):
        info = MagicMock()
        info.resolved_parent_context = ("outer-workflow-run-1", "outer-node-execution-1")

        assert _resolve_workflow_parent_context(info) == info.resolved_parent_context
