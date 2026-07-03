from types import SimpleNamespace

from extensions import ext_workflow_log_publisher


class FakePublisher:
    def __init__(self) -> None:
        self.warm_up_calls = 0

    def warm_up(self) -> None:
        self.warm_up_calls += 1


def test_workflow_log_publisher_extension_skips_when_disabled(monkeypatch) -> None:
    publisher = FakePublisher()
    monkeypatch.setattr(ext_workflow_log_publisher.dify_config, "WORKFLOW_LOG_ASYNC_ENABLED", False)
    monkeypatch.setattr(ext_workflow_log_publisher, "get_workflow_node_execution_activemq_publisher", lambda: publisher)

    ext_workflow_log_publisher.init_app(SimpleNamespace())

    assert publisher.warm_up_calls == 0


def test_workflow_log_publisher_extension_warms_when_enabled(monkeypatch) -> None:
    publisher = FakePublisher()
    monkeypatch.setattr(ext_workflow_log_publisher.dify_config, "WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr(ext_workflow_log_publisher.dify_config, "WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")
    monkeypatch.setattr(ext_workflow_log_publisher, "get_workflow_node_execution_activemq_publisher", lambda: publisher)

    ext_workflow_log_publisher.init_app(SimpleNamespace())

    assert publisher.warm_up_calls == 1
