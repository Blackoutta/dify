import json
import logging
from math import ceil

from celery import shared_task  # type: ignore
from celery.exceptions import Retry
from flask import current_app

from configs import dify_config
from core.ops.entities.config_entity import OPS_FILE_PATH, OPS_TRACE_FAILED_KEY
from core.ops.entities.trace_entity import trace_info_info_map
from core.ops.exceptions import PendingTraceParentContextError
from core.rag.models.document import Document
from extensions.ext_redis import redis_client
from extensions.ext_storage import storage
from models.model import Message
from models.workflow import WorkflowRun

logger = logging.getLogger(__name__)

_RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS = dify_config.OPS_TRACE_RETRYABLE_DISPATCH_DELAY_SECONDS
_RETRYABLE_TRACE_DISPATCH_LIMIT = max(
    dify_config.OPS_TRACE_RETRYABLE_DISPATCH_MAX_RETRIES,
    ceil(dify_config.WORKFLOW_MAX_EXECUTION_TIME / _RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS),
)


@shared_task(
    queue="ops_trace",
    bind=True,
    max_retries=_RETRYABLE_TRACE_DISPATCH_LIMIT,
    default_retry_delay=_RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS,
)
def process_trace_tasks(self, file_info):
    """
    Async process trace tasks
    Usage: process_trace_tasks.delay(tasks_data)
    """
    from core.ops.ops_trace_manager import OpsTraceManager

    app_id = file_info.get("app_id")
    file_id = file_info.get("file_id")
    file_path = f"{OPS_FILE_PATH}{app_id}/{file_id}.json"
    file_data = json.loads(storage.load(file_path))
    trace_info = file_data.get("trace_info")
    trace_info_type = file_data.get("trace_info_type")
    trace_instance = OpsTraceManager.get_ops_trace_instance(app_id)

    if trace_info.get("message_data"):
        trace_info["message_data"] = Message.from_dict(data=trace_info["message_data"])
    if trace_info.get("workflow_data") and not trace_info.get("workflow_snapshot"):
        trace_info["workflow_data"] = WorkflowRun.from_dict(data=trace_info["workflow_data"])
    if trace_info.get("documents"):
        trace_info["documents"] = [Document(**doc) for doc in trace_info["documents"]]

    should_delete_file = True

    try:
        if trace_instance:
            with current_app.app_context():
                trace_type = trace_info_info_map.get(trace_info_type)
                if trace_type:
                    trace_info = trace_type(**trace_info)
                trace_instance.trace(trace_info)
        logger.info("Processing trace tasks success, app_id: %s", app_id)
    except PendingTraceParentContextError as e:
        if self.request.retries >= _RETRYABLE_TRACE_DISPATCH_LIMIT:
            logger.exception("Pending trace parent context retry budget exhausted, app_id: %s", app_id)
            failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
            redis_client.incr(failed_key)
        else:
            logger.info(
                "Pending trace parent context, scheduling retry %s/%s for app_id %s: %s",
                self.request.retries + 1,
                _RETRYABLE_TRACE_DISPATCH_LIMIT,
                app_id,
                e,
            )
            try:
                raise self.retry(exc=e, countdown=_RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS)
            except Retry:
                should_delete_file = False
                raise
            except Exception:
                logger.exception("Failed to schedule pending parent context retry, app_id: %s", app_id)
                failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
                redis_client.incr(failed_key)
    except Exception as e:
        logger.info(
            f"error:\n\n\n{e}\n\n\n\n",
        )
        failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
        redis_client.incr(failed_key)
        logger.info("Processing trace tasks failed, app_id: %s", app_id)
    finally:
        if should_delete_file:
            storage.delete(file_path)
