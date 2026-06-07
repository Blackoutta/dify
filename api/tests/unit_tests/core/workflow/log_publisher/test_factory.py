from unittest.mock import Mock

import pytest

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.factory import create_workflow_log_publisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher


def test_factory_returns_noop_when_async_disabled():
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=False)

    publisher = create_workflow_log_publisher(config)

    assert isinstance(publisher, NoopWorkflowLogPublisher)


def _activemq_config(**overrides):
    values = {
        "WORKFLOW_LOG_ASYNC_ENABLED": True,
        "WORKFLOW_LOG_QUEUE_PROVIDER": "activemq",
        "WORKFLOW_LOG_ACTIVEMQ_HOST": "mq.local",
        "WORKFLOW_LOG_ACTIVEMQ_PORT": 61613,
        "WORKFLOW_LOG_ACTIVEMQ_USERNAME": "user",
        "WORKFLOW_LOG_ACTIVEMQ_PASSWORD": "pass",
        "WORKFLOW_LOG_ACTIVEMQ_DESTINATION": "/queue/dify.workflow.logs",
        "WORKFLOW_LOG_PUBLISH_TIMEOUT": 0.2,
        "WORKFLOW_LOG_PUBLISH_MAX_RETRIES": 1,
        "WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD": 0.5,
    }
    values.update(overrides)
    return Mock(**values)


def test_factory_returns_activemq_when_enabled():
    publisher = create_workflow_log_publisher(_activemq_config())

    assert isinstance(publisher, ActiveMQWorkflowLogPublisher)


def test_factory_rejects_unsupported_provider():
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=True, WORKFLOW_LOG_QUEUE_PROVIDER="kafka")

    with pytest.raises(ValueError, match="Unsupported workflow log queue provider"):
        create_workflow_log_publisher(config)


def test_factory_reuses_process_singleton_for_same_activemq_config():
    first = create_workflow_log_publisher(_activemq_config())
    second = create_workflow_log_publisher(_activemq_config())

    assert first is second


def test_factory_creates_new_singleton_when_slow_log_threshold_changes():
    first = create_workflow_log_publisher(_activemq_config(WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD=0.1))
    second = create_workflow_log_publisher(_activemq_config(WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD=0.2))

    assert first is not second


def test_factory_creates_new_singleton_when_activemq_config_changes():
    first = create_workflow_log_publisher(_activemq_config(WORKFLOW_LOG_ACTIVEMQ_HOST="mq-a.local"))
    second = create_workflow_log_publisher(_activemq_config(WORKFLOW_LOG_ACTIVEMQ_HOST="mq-b.local"))

    assert first is not second
