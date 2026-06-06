from __future__ import annotations

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher


def create_workflow_log_publisher(config) -> WorkflowLogPublisher:
    if not config.WORKFLOW_LOG_ASYNC_ENABLED:
        return NoopWorkflowLogPublisher()

    provider = str(config.WORKFLOW_LOG_QUEUE_PROVIDER).lower()
    if provider != "activemq":
        raise ValueError(f"Unsupported workflow log queue provider: {config.WORKFLOW_LOG_QUEUE_PROVIDER}")

    return ActiveMQWorkflowLogPublisher(
        host=config.WORKFLOW_LOG_ACTIVEMQ_HOST,
        port=config.WORKFLOW_LOG_ACTIVEMQ_PORT,
        username=config.WORKFLOW_LOG_ACTIVEMQ_USERNAME,
        password=config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD,
        destination=config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
        timeout=config.WORKFLOW_LOG_PUBLISH_TIMEOUT,
    )
