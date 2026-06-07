from core.ops.entities.trace_entity import WorkflowTraceInfo


def test_workflow_trace_info_keeps_node_snapshots_json_safe():
    trace_info = WorkflowTraceInfo(
        workflow_data={},
        conversation_id=None,
        workflow_id="workflow-1",
        tenant_id="tenant-1",
        workflow_run_id="run-1",
        workflow_run_elapsed_time=1.2,
        workflow_run_status="succeeded",
        workflow_run_inputs={"query": "hello"},
        workflow_run_outputs={"answer": "world"},
        workflow_run_version="1",
        error=None,
        total_tokens=10,
        file_list=[],
        query="hello",
        metadata={"app_id": "app-1"},
        node_execution_snapshots=[
            {
                "id": "record-1",
                "workflow_run_id": "run-1",
                "node_execution_id": "node-exec-1",
                "node_id": "llm",
                "node_type": "llm",
                "title": "LLM",
                "inputs": {"query": "hello"},
                "process_data": {"prompts": []},
                "outputs": {"text": "world"},
                "status": "succeeded",
                "error": None,
                "elapsed_time": 1.0,
                "metadata": {"total_tokens": 10},
                "created_at": "2026-06-06T01:02:03Z",
                "finished_at": "2026-06-06T01:02:04Z",
            }
        ],
    )

    restored = WorkflowTraceInfo.model_validate_json(trace_info.model_dump_json())

    assert restored.node_execution_snapshots[0]["node_execution_id"] == "node-exec-1"
