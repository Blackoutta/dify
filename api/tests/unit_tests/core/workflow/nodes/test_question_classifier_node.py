from unittest import mock

from core.model_runtime.entities import ImagePromptMessageContent
from core.workflow.entities import GraphInitParams
from core.workflow.nodes.llm.file_saver import LLMFileSaver
from core.workflow.nodes.question_classifier import QuestionClassifierNode, QuestionClassifierNodeData


def _create_question_classifier_node(*, retry_enabled: bool) -> QuestionClassifierNode:
    data = {
        "title": "test classifier node",
        "query_variable_selector": ["sys", "query"],
        "model": {
            "provider": "openai",
            "name": "gpt-3.5-turbo",
            "mode": "chat",
            "completion_params": {},
        },
        "classes": [{"id": "1", "name": "class 1"}],
        "retry_config": {
            "retry_enabled": retry_enabled,
            "max_retries": 3,
            "retry_interval": 1000,
        },
    }
    return QuestionClassifierNode(
        id="classifier",
        config={"id": "classifier", "data": data},
        graph_init_params=GraphInitParams(
            tenant_id="tenant",
            app_id="app",
            workflow_id="workflow",
            graph_config={},
            user_id="user",
            user_from="account",
            invoke_from="debugger",
            call_depth=0,
        ),
        graph_runtime_state=mock.MagicMock(),
        llm_file_saver=mock.MagicMock(spec=LLMFileSaver),
    )


def test_question_classifier_retry_reflects_config():
    assert _create_question_classifier_node(retry_enabled=False).retry is False
    assert _create_question_classifier_node(retry_enabled=True).retry is True


def test_init_question_classifier_node_data():
    data = {
        "title": "test classifier node",
        "query_variable_selector": ["id", "name"],
        "model": {"provider": "openai", "name": "gpt-3.5-turbo", "mode": "completion", "completion_params": {}},
        "classes": [{"id": "1", "name": "class 1"}],
        "instruction": "This is a test instruction",
        "memory": {
            "role_prefix": {"user": "Human:", "assistant": "AI:"},
            "window": {"enabled": True, "size": 5},
            "query_prompt_template": "Previous conversation:\n{history}\n\nHuman: {query}\nAI:",
        },
        "vision": {"enabled": True, "configs": {"variable_selector": ["image"], "detail": "low"}},
    }

    node_data = QuestionClassifierNodeData.model_validate(data)

    assert node_data.query_variable_selector == ["id", "name"]
    assert node_data.model.provider == "openai"
    assert node_data.classes[0].id == "1"
    assert node_data.instruction == "This is a test instruction"
    assert node_data.memory is not None
    assert node_data.memory.role_prefix is not None
    assert node_data.memory.role_prefix.user == "Human:"
    assert node_data.memory.role_prefix.assistant == "AI:"
    assert node_data.memory.window.enabled == True
    assert node_data.memory.window.size == 5
    assert node_data.memory.query_prompt_template == "Previous conversation:\n{history}\n\nHuman: {query}\nAI:"
    assert node_data.vision.enabled == True
    assert node_data.vision.configs.variable_selector == ["image"]
    assert node_data.vision.configs.detail == ImagePromptMessageContent.DETAIL.LOW


def test_init_question_classifier_node_data_without_vision_config():
    data = {
        "title": "test classifier node",
        "query_variable_selector": ["id", "name"],
        "model": {"provider": "openai", "name": "gpt-3.5-turbo", "mode": "completion", "completion_params": {}},
        "classes": [{"id": "1", "name": "class 1"}],
        "instruction": "This is a test instruction",
        "memory": {
            "role_prefix": {"user": "Human:", "assistant": "AI:"},
            "window": {"enabled": True, "size": 5},
            "query_prompt_template": "Previous conversation:\n{history}\n\nHuman: {query}\nAI:",
        },
    }

    node_data = QuestionClassifierNodeData.model_validate(data)

    assert node_data.query_variable_selector == ["id", "name"]
    assert node_data.model.provider == "openai"
    assert node_data.classes[0].id == "1"
    assert node_data.instruction == "This is a test instruction"
    assert node_data.memory is not None
    assert node_data.memory.role_prefix is not None
    assert node_data.memory.role_prefix.user == "Human:"
    assert node_data.memory.role_prefix.assistant == "AI:"
    assert node_data.memory.window.enabled == True
    assert node_data.memory.window.size == 5
    assert node_data.memory.query_prompt_template == "Previous conversation:\n{history}\n\nHuman: {query}\nAI:"
    assert node_data.vision.enabled == False
    assert node_data.vision.configs.variable_selector == ["sys", "files"]
    assert node_data.vision.configs.detail == ImagePromptMessageContent.DETAIL.HIGH
    assert node_data.retry_config.retry_enabled is False
