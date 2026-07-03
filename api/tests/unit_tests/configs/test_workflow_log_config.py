from configs.feature import WorkflowLogConfig


def test_async_workflow_log_defaults_are_disabled(monkeypatch) -> None:
    for key in (
        "WORKFLOW_LOG_ASYNC_ENABLED",
        "WORKFLOW_LOG_QUEUE_PROVIDER",
        "WORKFLOW_LOG_ACTIVEMQ_HOST",
        "WORKFLOW_LOG_ACTIVEMQ_PORT",
        "WORKFLOW_LOG_ACTIVEMQ_USERNAME",
        "WORKFLOW_LOG_ACTIVEMQ_PASSWORD",
        "WORKFLOW_LOG_ACTIVEMQ_DESTINATION",
        "WORKFLOW_LOG_PUBLISH_TIMEOUT",
        "WORKFLOW_LOG_PUBLISH_MAX_RETRIES",
    ):
        monkeypatch.delenv(key, raising=False)

    config = WorkflowLogConfig(_env_file=None)

    assert config.WORKFLOW_LOG_ASYNC_ENABLED is False
    assert config.WORKFLOW_LOG_QUEUE_PROVIDER == "activemq"
    assert config.WORKFLOW_LOG_ACTIVEMQ_HOST == "localhost"
    assert config.WORKFLOW_LOG_ACTIVEMQ_PORT == 61613
    assert config.WORKFLOW_LOG_ACTIVEMQ_USERNAME == ""
    assert config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD == ""
    assert config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION == "/queue/dify.workflow.logs"
    assert config.WORKFLOW_LOG_PUBLISH_TIMEOUT == 0.2
    assert config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES == 1
