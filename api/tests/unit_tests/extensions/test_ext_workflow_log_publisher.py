import logging
from unittest.mock import Mock

from extensions import ext_workflow_log_publisher


class DummyApp:
    pass


def test_init_app_warms_up_async_workflow_log_publisher(monkeypatch):
    publisher = Mock()
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=True, WORKFLOW_LOG_QUEUE_PROVIDER="activemq")
    create_publisher = Mock(return_value=publisher)
    monkeypatch.setattr(ext_workflow_log_publisher, "dify_config", config)
    monkeypatch.setattr(ext_workflow_log_publisher, "create_workflow_log_publisher", create_publisher)

    ext_workflow_log_publisher.init_app(DummyApp())

    create_publisher.assert_called_once_with(config)
    publisher.warm_up.assert_called_once_with()


def test_init_app_skips_warm_up_when_async_disabled(monkeypatch):
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=False, WORKFLOW_LOG_QUEUE_PROVIDER="activemq")
    create_publisher = Mock()
    monkeypatch.setattr(ext_workflow_log_publisher, "dify_config", config)
    monkeypatch.setattr(ext_workflow_log_publisher, "create_workflow_log_publisher", create_publisher)

    ext_workflow_log_publisher.init_app(DummyApp())

    create_publisher.assert_not_called()


def test_init_app_logs_warning_when_warm_up_fails(monkeypatch, caplog):
    publisher = Mock()
    publisher.warm_up.side_effect = RuntimeError("broker down")
    config = Mock(WORKFLOW_LOG_ASYNC_ENABLED=True, WORKFLOW_LOG_QUEUE_PROVIDER="activemq")
    create_publisher = Mock(return_value=publisher)
    monkeypatch.setattr(ext_workflow_log_publisher, "dify_config", config)
    monkeypatch.setattr(ext_workflow_log_publisher, "create_workflow_log_publisher", create_publisher)

    with caplog.at_level(logging.WARNING, logger="extensions.ext_workflow_log_publisher"):
        ext_workflow_log_publisher.init_app(DummyApp())

    assert any("Failed to warm up workflow log publisher" in record.message for record in caplog.records)


def test_is_enabled_only_for_activemq_async(monkeypatch):
    monkeypatch.setattr(
        ext_workflow_log_publisher,
        "dify_config",
        Mock(WORKFLOW_LOG_ASYNC_ENABLED=True, WORKFLOW_LOG_QUEUE_PROVIDER="activemq"),
    )

    assert ext_workflow_log_publisher.is_enabled() is True


def test_is_enabled_false_for_disabled_async(monkeypatch):
    monkeypatch.setattr(
        ext_workflow_log_publisher,
        "dify_config",
        Mock(WORKFLOW_LOG_ASYNC_ENABLED=False, WORKFLOW_LOG_QUEUE_PROVIDER="activemq"),
    )

    assert ext_workflow_log_publisher.is_enabled() is False
