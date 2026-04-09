from collections.abc import Generator

from core.app.apps.advanced_chat.generate_response_converter import AdvancedChatAppGenerateResponseConverter
from core.app.entities.task_entities import (
    ChatbotAppBlockingResponse,
    ChatbotAppPausedBlockingResponse,
    ChatbotAppStreamResponse,
    ErrorStreamResponse,
    HumanInputRequiredResponse,
    MessageEndStreamResponse,
    NodeFinishStreamResponse,
    NodeStartStreamResponse,
    PingStreamResponse,
)
from dify_graph.entities.pause_reason import PauseReasonType
from dify_graph.enums import WorkflowExecutionStatus, WorkflowNodeExecutionStatus


class TestAdvancedChatGenerateResponseConverter:
    def test_blocking_simple_response_metadata(self):
        data = ChatbotAppBlockingResponse.Data(
            id="msg-1",
            mode="chat",
            conversation_id="c1",
            message_id="m1",
            answer="hi",
            metadata={"usage": {"total_tokens": 1}},
            created_at=1,
        )
        blocking = ChatbotAppBlockingResponse(task_id="t1", data=data)
        response = AdvancedChatAppGenerateResponseConverter.convert_blocking_simple_response(blocking)
        assert "usage" not in response["metadata"]

    def test_blocking_full_response_converts_pause_payload(self):
        data = ChatbotAppPausedBlockingResponse.Data(
            id="msg-1",
            mode="chat",
            conversation_id="c1",
            message_id="m1",
            workflow_run_id="run-1",
            answer="partial",
            metadata={"usage": {"total_tokens": 1}},
            created_at=1,
            paused_nodes=["node-1"],
            reasons=[{"TYPE": PauseReasonType.HUMAN_INPUT_REQUIRED, "form_id": "form-1"}],
            human_input_forms=[
                HumanInputRequiredResponse.Data(
                    form_id="form-1",
                    node_id="node-1",
                    node_title="Approval",
                    form_content="Need approval",
                    inputs=[],
                    actions=[],
                    display_in_ui=True,
                    form_token="token-1",
                    resolved_default_values={},
                    expiration_time=100,
                )
            ],
            status=WorkflowExecutionStatus.PAUSED,
            elapsed_time=0.1,
            total_tokens=0,
            total_steps=0,
        )
        blocking = ChatbotAppPausedBlockingResponse(task_id="t1", data=data)

        response = AdvancedChatAppGenerateResponseConverter.convert_blocking_full_response(blocking)

        assert response["event"] == "workflow_paused"
        assert response["workflow_run_id"] == "run-1"
        assert response["answer"] == "partial"
        assert response["data"]["human_input_forms"][0]["expiration_time"] == 100

    def test_stream_simple_response_includes_node_events(self):
        node_start = NodeStartStreamResponse(
            task_id="t1",
            workflow_run_id="r1",
            data=NodeStartStreamResponse.Data(
                id="e1",
                node_id="n1",
                node_type="answer",
                title="Answer",
                index=1,
                created_at=1,
            ),
        )
        node_finish = NodeFinishStreamResponse(
            task_id="t1",
            workflow_run_id="r1",
            data=NodeFinishStreamResponse.Data(
                id="e1",
                node_id="n1",
                node_type="answer",
                title="Answer",
                index=1,
                status=WorkflowNodeExecutionStatus.SUCCEEDED,
                elapsed_time=0.1,
                created_at=1,
                finished_at=2,
            ),
        )

        def stream() -> Generator[ChatbotAppStreamResponse, None, None]:
            yield ChatbotAppStreamResponse(
                conversation_id="c1",
                message_id="m1",
                created_at=1,
                stream_response=PingStreamResponse(task_id="t1"),
            )
            yield ChatbotAppStreamResponse(
                conversation_id="c1",
                message_id="m1",
                created_at=1,
                stream_response=node_start,
            )
            yield ChatbotAppStreamResponse(
                conversation_id="c1",
                message_id="m1",
                created_at=1,
                stream_response=node_finish,
            )
            yield ChatbotAppStreamResponse(
                conversation_id="c1",
                message_id="m1",
                created_at=1,
                stream_response=ErrorStreamResponse(task_id="t1", err=ValueError("boom")),
            )
            yield ChatbotAppStreamResponse(
                conversation_id="c1",
                message_id="m1",
                created_at=1,
                stream_response=MessageEndStreamResponse(task_id="t1", id="m1"),
            )

        converted = list(AdvancedChatAppGenerateResponseConverter.convert_stream_simple_response(stream()))
        assert converted[0] == "ping"
        assert converted[1]["event"] == "node_started"
        assert converted[2]["event"] == "node_finished"
        assert converted[3]["event"] == "error"
