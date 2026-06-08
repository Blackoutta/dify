import logging
from collections.abc import Generator, Iterable, Mapping
from typing import Any, Optional

from core.callback_handler.agent_tool_callback_handler import DifyAgentCallbackHandler
from core.ops.ops_trace_manager import TraceQueueManager
from core.tools.entities.tool_entities import ToolInvokeMessage

logger = logging.getLogger(__name__)


class DifyWorkflowCallbackHandler(DifyAgentCallbackHandler):
    """Callback Handler that prints to std out."""

    def on_tool_execution(
        self,
        tool_name: str,
        tool_inputs: Mapping[str, Any],
        tool_outputs: Iterable[ToolInvokeMessage],
        message_id: Optional[str] = None,
        timer: Optional[Any] = None,
        trace_manager: Optional[TraceQueueManager] = None,
    ) -> Generator[ToolInvokeMessage, None, None]:
        for tool_output in tool_outputs:
            logger.debug(
                "[on_tool_execution]\nTool: %s\nOutputs: %s",
                tool_name,
                tool_output.model_dump_json()[:1000],
            )
            yield tool_output
