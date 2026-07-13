import logging

from configs import dify_config
from core.repositories.workflow_node_execution_activemq_repository import (
    get_workflow_node_execution_activemq_publisher,
)
from dify_app import DifyApp

logger = logging.getLogger(__name__)


def is_enabled() -> bool:
    provider = str(dify_config.WORKFLOW_LOG_QUEUE_PROVIDER).lower()
    return bool(dify_config.WORKFLOW_LOG_ASYNC_ENABLED) and provider == "activemq"


def init_app(app: DifyApp) -> None:
    if not is_enabled():
        return

    try:
        get_workflow_node_execution_activemq_publisher().warm_up()
        logger.info("Warmed up workflow node execution ActiveMQ publisher")
    except Exception:
        logger.warning("Failed to warm up workflow node execution ActiveMQ publisher", exc_info=True)
