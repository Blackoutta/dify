import json

import pytest
from celery.exceptions import Retry

from configs import dify_config
from core.ops.exceptions import PendingTraceParentContextError, RetryableTraceDispatchError
from tasks.ops_trace_task import (
    _RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS,
    _RETRYABLE_TRACE_DISPATCH_LIMIT,
    process_trace_tasks,
)


class RetryableProvider:
    def trace(self, trace_info):
        raise PendingTraceParentContextError("outer-run:tool-node")


class GenericRetryableProvider:
    def trace(self, trace_info):
        raise RetryableTraceDispatchError("generic retryable error")


class SuccessfulProvider:
    def trace(self, trace_info):
        return None


def _patch_payload(monkeypatch, provider, file_data):
    calls = {"deleted": False, "saved": None, "failed_count": 0}

    monkeypatch.setattr("tasks.ops_trace_task.storage.load", lambda path: json.dumps(file_data))
    monkeypatch.setattr("tasks.ops_trace_task.storage.delete", lambda path: calls.__setitem__("deleted", True))
    monkeypatch.setattr("tasks.ops_trace_task.storage.save", lambda path, data: calls.__setitem__("saved", data))
    monkeypatch.setattr("tasks.ops_trace_task.redis_client.incr", lambda key: calls.__setitem__("failed_count", 1))
    monkeypatch.setattr("core.ops.ops_trace_manager.OpsTraceManager.get_ops_trace_instance", lambda app_id: provider)
    return calls


def test_retryable_trace_dispatch_keeps_payload_when_retry_is_scheduled(monkeypatch):
    file_data = {"trace_info_type": "BaseTraceInfo", "trace_info": {"metadata": {}}}
    calls = _patch_payload(monkeypatch, RetryableProvider(), file_data)

    def fake_retry(exc, countdown):
        raise Retry()

    process_trace_tasks.request.retries = 0
    monkeypatch.setattr(process_trace_tasks, "retry", fake_retry)

    with pytest.raises(Retry):
        process_trace_tasks.run({"app_id": "app-id", "file_id": "file-id"})

    assert calls["deleted"] is False
    assert calls["failed_count"] == 0


def test_retryable_trace_dispatch_deletes_payload_after_budget_exhausted(monkeypatch):
    file_data = {"trace_info_type": "BaseTraceInfo", "trace_info": {"metadata": {}}}
    calls = _patch_payload(monkeypatch, RetryableProvider(), file_data)

    process_trace_tasks.request.retries = 60

    process_trace_tasks.run({"app_id": "app-id", "file_id": "file-id"})

    assert calls["deleted"] is True
    assert calls["failed_count"] == 1


def test_generic_retryable_trace_dispatch_does_not_schedule_retry(monkeypatch):
    file_data = {"trace_info_type": "BaseTraceInfo", "trace_info": {"metadata": {}}}
    calls = _patch_payload(monkeypatch, GenericRetryableProvider(), file_data)

    def fail_retry(exc, countdown):
        pytest.fail("generic retryable dispatch errors should not schedule Celery retry")

    process_trace_tasks.request.retries = 0
    monkeypatch.setattr(process_trace_tasks, "retry", fail_retry)

    process_trace_tasks.run({"app_id": "app-id", "file_id": "file-id"})

    assert calls["deleted"] is True
    assert calls["failed_count"] == 1


def test_retryable_trace_dispatch_budget_covers_workflow_execution_window():
    assert dify_config.OPS_TRACE_RETRYABLE_DISPATCH_MAX_RETRIES == 60
    assert _RETRYABLE_TRACE_DISPATCH_LIMIT * _RETRYABLE_TRACE_DISPATCH_DELAY_SECONDS >= (
        dify_config.WORKFLOW_MAX_EXECUTION_TIME
    )
    assert process_trace_tasks.max_retries == _RETRYABLE_TRACE_DISPATCH_LIMIT
