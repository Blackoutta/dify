import logging

from configs import dify_config
from core.workflow.log_publisher import create_workflow_log_publisher
from dify_app import DifyApp

logger = logging.getLogger(__name__)


def is_enabled() -> bool:
    provider = str(dify_config.WORKFLOW_LOG_QUEUE_PROVIDER).lower()
    return bool(dify_config.WORKFLOW_LOG_ASYNC_ENABLED) and provider == "activemq"


def init_app(app: DifyApp):
    if not is_enabled():
        return

    try:
        publisher = create_workflow_log_publisher(dify_config)
        publisher.warm_up()
        logger.info("Warmed up workflow log publisher")
    except Exception:
        logger.warning("Failed to warm up workflow log publisher", exc_info=True)
