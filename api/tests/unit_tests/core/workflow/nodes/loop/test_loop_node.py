import time

from core.app.entities.app_invoke_entities import InvokeFrom
from core.workflow.entities.variable_pool import VariablePool
from core.workflow.graph_engine.entities.graph import Graph
from core.workflow.graph_engine.entities.graph_init_params import GraphInitParams
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.nodes.loop.loop_node import LoopNode
from models.enums import UserFrom
from models.workflow import WorkflowType


class CacheCaptureGraphEngine:
    captured_graph_runtime_state = None

    def __init__(self, *args, **kwargs):
        self.__class__.captured_graph_runtime_state = kwargs["graph_runtime_state"]
        self.graph_runtime_state = kwargs["graph_runtime_state"]

    def run(self):
        return iter(())


def test_loop_subgraph_reuses_parent_workflow_tool_runtime_cache(monkeypatch):
    parent_state = GraphRuntimeState(
        variable_pool=VariablePool(system_variables={}, user_inputs={}),
        start_at=time.perf_counter(),
    )

    graph_config = {
        "nodes": [
            {
                "id": "loop-1",
                "data": {
                    "title": "loop",
                    "type": "loop",
                    "start_node_id": "inner",
                    "startNodeType": "template-transform",
                    "break_conditions": [],
                    "loop_count": 1,
                    "logical_operator": "and",
                    "loop_variables": [],
                },
            },
            {
                "id": "inner",
                "data": {
                    "title": "inner",
                    "type": "template-transform",
                    "loop_id": "loop-1",
                    "template": "ok",
                    "variables": [],
                },
            },
        ],
        "edges": [],
    }
    graph = Graph.init(graph_config=graph_config, root_node_id="loop-1")
    init_params = GraphInitParams(
        tenant_id="1",
        app_id="1",
        workflow_type=WorkflowType.CHAT,
        workflow_id="1",
        graph_config=graph_config,
        user_id="1",
        user_from=UserFrom.ACCOUNT,
        invoke_from=InvokeFrom.DEBUGGER,
        call_depth=0,
    )
    loop_node = LoopNode(
        id="loop-1",
        config=graph_config["nodes"][0],
        graph_init_params=init_params,
        graph=graph,
        graph_runtime_state=parent_state,
    )

    CacheCaptureGraphEngine.captured_graph_runtime_state = None
    monkeypatch.setattr("core.workflow.graph_engine.graph_engine.GraphEngine", CacheCaptureGraphEngine)

    list(loop_node._run())

    assert CacheCaptureGraphEngine.captured_graph_runtime_state is not None
    assert (
        CacheCaptureGraphEngine.captured_graph_runtime_state.workflow_tool_runtime_cache
        is parent_state.workflow_tool_runtime_cache
    )
