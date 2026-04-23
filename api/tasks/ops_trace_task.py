"""
Celery task for asynchronous ops trace dispatch.

Phoenix nested workflow traces can arrive before their parent tool span context is
published to Redis. That ordering window is transient, so this task retries only
that specific failure mode and preserves the payload file while the retry is
scheduled. All other failures remain terminal and clean up the stored payload.
"""

import json
import logging

from celery import shared_task
from celery.exceptions import Retry
from flask import current_app

from core.ops.entities.config_entity import OPS_FILE_PATH, OPS_TRACE_FAILED_KEY
from core.ops.entities.trace_entity import trace_info_info_map
from core.rag.models.document import Document
from extensions.ext_redis import redis_client
from extensions.ext_storage import storage
from models.model import Message
from models.workflow import WorkflowRun

logger = logging.getLogger(__name__)

_PENDING_PHOENIX_PARENT_RETRY_LIMIT = 3
_PENDING_PHOENIX_PARENT_RETRY_DELAY_SECONDS = 5


@shared_task(
    queue="ops_trace",
    bind=True,
    max_retries=_PENDING_PHOENIX_PARENT_RETRY_LIMIT,
    default_retry_delay=_PENDING_PHOENIX_PARENT_RETRY_DELAY_SECONDS,
)
def process_trace_tasks(self, file_info):
    """
    Async process trace tasks
    Usage: process_trace_tasks.delay(tasks_data)
    """
    from core.ops.ops_trace_manager import OpsTraceManager
    from dify_trace_arize_phoenix.arize_phoenix_trace import PendingPhoenixParentSpanContextError

    app_id = file_info.get("app_id")
    file_id = file_info.get("file_id")
    file_path = f"{OPS_FILE_PATH}{app_id}/{file_id}.json"
    file_data = json.loads(storage.load(file_path))
    trace_info = file_data.get("trace_info")
    trace_info_type = file_data.get("trace_info_type")
    trace_instance = OpsTraceManager.get_ops_trace_instance(app_id)

    if trace_info.get("message_data"):
        trace_info["message_data"] = Message.from_dict(data=trace_info["message_data"])
    if trace_info.get("workflow_data"):
        trace_info["workflow_data"] = WorkflowRun.from_dict(data=trace_info["workflow_data"])
    if trace_info.get("documents"):
        trace_info["documents"] = [Document.model_validate(doc) for doc in trace_info["documents"]]

    should_delete_file = True

    try:
        trace_type = trace_info_info_map.get(trace_info_type)
        if trace_type:
            trace_info = trace_type(**trace_info)

        from extensions.ext_enterprise_telemetry import is_enabled as is_ee_telemetry_enabled

        if is_ee_telemetry_enabled():
            from enterprise.telemetry.enterprise_trace import EnterpriseOtelTrace

            try:
                EnterpriseOtelTrace().trace(trace_info)
            except Exception:
                logger.exception("Enterprise trace failed for app_id: %s", app_id)

        if trace_instance:
            with current_app.app_context():
                trace_instance.trace(trace_info)

        logger.info("Processing trace tasks success, app_id: %s", app_id)
    except PendingPhoenixParentSpanContextError as e:
        if self.request.retries >= _PENDING_PHOENIX_PARENT_RETRY_LIMIT:
            logger.exception("Phoenix parent span context retry budget exhausted, app_id: %s", app_id)
            failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
            redis_client.incr(failed_key)
        else:
            logger.warning(
                "Phoenix parent span context pending, scheduling retry %s/%s for app_id %s",
                self.request.retries + 1,
                _PENDING_PHOENIX_PARENT_RETRY_LIMIT,
                app_id,
            )
            try:
                raise self.retry(exc=e, countdown=_PENDING_PHOENIX_PARENT_RETRY_DELAY_SECONDS)
            except Retry:
                should_delete_file = False
                raise
            except Exception:
                logger.exception("Failed to schedule Phoenix parent span context retry, app_id: %s", app_id)
                failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
                redis_client.incr(failed_key)
    except Exception as e:
        logger.exception("Processing trace tasks failed, app_id: %s", app_id)
        failed_key = f"{OPS_TRACE_FAILED_KEY}_{app_id}"
        redis_client.incr(failed_key)
    finally:
        if should_delete_file:
            try:
                storage.delete(file_path)
            except Exception as e:
                logger.warning(
                    "Failed to delete trace file %s for app_id %s: %s",
                    file_path,
                    app_id,
                    e,
                )
