from __future__ import annotations

import atexit
import threading
from dataclasses import dataclass

from core.workflow.log_publisher.activemq_publisher import ActiveMQWorkflowLogPublisher
from core.workflow.log_publisher.publisher import NoopWorkflowLogPublisher, WorkflowLogPublisher


@dataclass(frozen=True)
class _ActiveMQPublisherConfigKey:
    host: str
    port: int
    username: str | None
    password: str | None
    destination: str
    timeout: float
    max_retries: int


_singleton_lock = threading.RLock()
_singleton_publishers: dict[_ActiveMQPublisherConfigKey, ActiveMQWorkflowLogPublisher] = {}


def create_workflow_log_publisher(config) -> WorkflowLogPublisher:
    if not config.WORKFLOW_LOG_ASYNC_ENABLED:
        return NoopWorkflowLogPublisher()

    provider = str(config.WORKFLOW_LOG_QUEUE_PROVIDER).lower()
    if provider != "activemq":
        raise ValueError(f"Unsupported workflow log queue provider: {config.WORKFLOW_LOG_QUEUE_PROVIDER}")

    key = _ActiveMQPublisherConfigKey(
        host=config.WORKFLOW_LOG_ACTIVEMQ_HOST,
        port=config.WORKFLOW_LOG_ACTIVEMQ_PORT,
        username=config.WORKFLOW_LOG_ACTIVEMQ_USERNAME,
        password=config.WORKFLOW_LOG_ACTIVEMQ_PASSWORD,
        destination=config.WORKFLOW_LOG_ACTIVEMQ_DESTINATION,
        timeout=config.WORKFLOW_LOG_PUBLISH_TIMEOUT,
        max_retries=config.WORKFLOW_LOG_PUBLISH_MAX_RETRIES,
    )

    with _singleton_lock:
        publisher = _singleton_publishers.get(key)
        if publisher is not None:
            return publisher

        publisher = ActiveMQWorkflowLogPublisher(
            host=key.host,
            port=key.port,
            username=key.username,
            password=key.password,
            destination=key.destination,
            timeout=key.timeout,
            max_retries=key.max_retries,
        )
        _singleton_publishers[key] = publisher
        atexit.register(publisher.close)
        return publisher
