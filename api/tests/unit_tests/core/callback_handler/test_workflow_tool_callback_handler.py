from core.callback_handler.workflow_tool_callback_handler import DifyWorkflowCallbackHandler
from core.tools.entities.tool_entities import ToolInvokeMessage


def test_workflow_tool_execution_logs_at_debug_instead_of_printing(caplog, capsys):
    handler = DifyWorkflowCallbackHandler()
    tool_output = ToolInvokeMessage(
        type=ToolInvokeMessage.MessageType.TEXT,
        message=ToolInvokeMessage.TextMessage(text='{"user_id": "load-test-user"}'),
    )

    with caplog.at_level("DEBUG", logger="core.callback_handler.workflow_tool_callback_handler"):
        outputs = list(handler.on_tool_execution("wf2_tool", {}, [tool_output]))

    captured = capsys.readouterr()

    assert outputs == [tool_output]
    assert captured.out == ""
    assert captured.err == ""
    assert "[on_tool_execution]" in caplog.text
    assert "Tool: wf2_tool" in caplog.text
    assert "user_id" in caplog.text
    assert "load-test-user" in caplog.text
