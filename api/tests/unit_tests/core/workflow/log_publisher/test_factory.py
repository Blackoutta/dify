from unittest.mock import Mock

import pytest

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.factory import create_workflow_log_publisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher


def test_factory_returns_noop_when_async_disabled():
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=False)

    publisher = create_workflow_log_publisher(config)

    assert isinstance(publisher, NoopWorkflowLogPublisher)


def test_factory_returns_activemq_when_enabled():
    config = Mock(
        WORKFLOW_LOG_ASYNC_ENABLED=True,
        WORKFLOW_LOG_QUEUE_PROVIDER="activemq",
        WORKFLOW_LOG_ACTIVEMQ_HOST="mq.local",
        WORKFLOW_LOG_ACTIVEMQ_PORT=61613,
        WORKFLOW_LOG_ACTIVEMQ_USERNAME="user",
        WORKFLOW_LOG_ACTIVEMQ_PASSWORD="pass",
        WORKFLOW_LOG_ACTIVEMQ_DESTINATION="/queue/dify.workflow.logs",
        WORKFLOW_LOG_PUBLISH_TIMEOUT=0.2,
    )

    publisher = create_workflow_log_publisher(config)

    assert isinstance(publisher, ActiveMQWorkflowLogPublisher)


def test_factory_rejects_unsupported_provider():
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=True, WORKFLOW_LOG_QUEUE_PROVIDER="kafka")

    with pytest.raises(ValueError, match="Unsupported workflow log queue provider"):
        create_workflow_log_publisher(config)
