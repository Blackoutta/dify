from datetime import UTC, datetime
from unittest.mock import Mock

import pytest

from core.app.workflow.layers.persistence import (
    PersistenceWorkflowInfo,
    WorkflowPersistenceLayer,
    _NodeRuntimeSnapshot,
)
from dify_graph.enums import WorkflowNodeExecutionStatus, WorkflowType
from dify_graph.node_events import NodeRunResult


def _build_layer(
    *,
    workflow_node_execution_repository: Mock | None = None,
    trace_manager: Mock | None = None,
) -> WorkflowPersistenceLayer:
    application_generate_entity = Mock()
    application_generate_entity.inputs = {}

    return WorkflowPersistenceLayer(
        application_generate_entity=application_generate_entity,
        workflow_info=PersistenceWorkflowInfo(
            workflow_id="workflow-id",
            workflow_type=WorkflowType.WORKFLOW,
            version="1",
            graph_data={},
        ),
        workflow_execution_repository=Mock(),
        workflow_node_execution_repository=workflow_node_execution_repository or Mock(),
        trace_manager=trace_manager,
    )


def test_update_node_execution_prefers_event_finished_at(monkeypatch: pytest.MonkeyPatch) -> None:
    layer = _build_layer()
    node_execution = Mock()
    node_execution.id = "node-exec-1"
    node_execution.created_at = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC).replace(tzinfo=None)
    node_execution.update_from_mapping = Mock()

    layer._node_snapshots[node_execution.id] = _NodeRuntimeSnapshot(
        node_id="node-id",
        title="LLM",
        predecessor_node_id=None,
        iteration_id="iter-1",
        loop_id=None,
        created_at=node_execution.created_at,
    )

    event_finished_at = datetime(2024, 1, 1, 0, 0, 2, tzinfo=UTC).replace(tzinfo=None)
    delayed_processing_time = datetime(2024, 1, 1, 0, 0, 10, tzinfo=UTC).replace(tzinfo=None)
    monkeypatch.setattr("core.app.workflow.layers.persistence.naive_utc_now", lambda: delayed_processing_time)

    layer._update_node_execution(
        node_execution,
        NodeRunResult(status=WorkflowNodeExecutionStatus.SUCCEEDED),
        WorkflowNodeExecutionStatus.SUCCEEDED,
        finished_at=event_finished_at,
    )

    assert node_execution.finished_at == event_finished_at
    assert node_execution.elapsed_time == 2.0


def test_enqueue_trace_task_passes_node_execution_snapshots() -> None:
    workflow_node_execution_repository = Mock()
    workflow_node_execution_repository.get_cached_executions_by_workflow_run.return_value = ["node-execution"]
    workflow_node_execution_repository.to_trace_snapshot.return_value = {"id": "row-id"}
    trace_manager = Mock()
    trace_manager.user_id = "user-id"
    layer = _build_layer(
        workflow_node_execution_repository=workflow_node_execution_repository,
        trace_manager=trace_manager,
    )
    layer._system_variables = Mock(return_value={})
    execution = Mock()
    execution.id_ = "run-id"

    layer._enqueue_trace_task(execution)

    trace_task = trace_manager.add_trace_task.call_args.args[0]
    assert trace_task.kwargs["node_execution_snapshots"] == [{"id": "row-id"}]
