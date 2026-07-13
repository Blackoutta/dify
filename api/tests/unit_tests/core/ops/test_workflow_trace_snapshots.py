from core.ops.entities.trace_entity import WorkflowTraceInfo
from core.ops.workflow_trace_snapshots import workflow_node_executions_from_snapshots
from dify_graph.enums import BuiltinNodeTypes, WorkflowNodeExecutionMetadataKey, WorkflowNodeExecutionStatus


def _trace_info(node_execution_snapshots: list[dict] | None = None) -> WorkflowTraceInfo:
    return WorkflowTraceInfo(
        workflow_id="workflow-id",
        tenant_id="tenant-id",
        workflow_run_id="run-id",
        workflow_run_elapsed_time=1.0,
        workflow_run_status="succeeded",
        workflow_run_inputs={},
        workflow_run_outputs={},
        workflow_run_version="1",
        total_tokens=0,
        file_list=[],
        query="",
        metadata={"app_id": "app-id"},
        node_execution_snapshots=node_execution_snapshots or [],
    )


def test_workflow_node_executions_from_snapshots_returns_none_without_snapshots() -> None:
    assert workflow_node_executions_from_snapshots(_trace_info()) is None


def test_workflow_node_snapshot_to_domain_like_coerces_provider_fields() -> None:
    executions = workflow_node_executions_from_snapshots(
        _trace_info(
            [
                {
                    "id": "row-id",
                    "tenant_id": "tenant-id",
                    "app_id": "app-id",
                    "workflow_id": "workflow-id",
                    "workflow_run_id": "run-id",
                    "node_execution_id": "node-exec-id",
                    "node_id": "llm",
                    "node_type": BuiltinNodeTypes.LLM,
                    "title": "LLM",
                    "inputs": {"prompt": "hello"},
                    "process_data": {},
                    "outputs": {"answer": "world"},
                    "status": "succeeded",
                    "error": None,
                    "elapsed_time": 1.2,
                    "metadata": {"total_tokens": 7},
                    "created_at": "2026-07-03T00:00:00Z",
                    "finished_at": "2026-07-03T00:00:01Z",
                }
            ]
        )
    )

    assert executions is not None
    execution = executions[0]
    assert execution.id == "row-id"
    assert execution.node_type == BuiltinNodeTypes.LLM
    assert execution.status == WorkflowNodeExecutionStatus.SUCCEEDED
    assert execution.metadata[WorkflowNodeExecutionMetadataKey.TOTAL_TOKENS] == 7
    assert execution.created_at.year == 2026
