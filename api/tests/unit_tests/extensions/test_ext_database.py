from types import SimpleNamespace
from unittest.mock import Mock

from extensions import ext_database


def test_is_gevent_hub_callback_detects_current_hub(monkeypatch):
    class FakeHub:
        pass

    monkeypatch.setattr(ext_database, "Hub", FakeHub)
    monkeypatch.setattr(ext_database, "getcurrent", lambda: FakeHub())

    assert ext_database._is_gevent_hub_callback()


def test_is_gevent_hub_callback_ignores_regular_greenlet(monkeypatch):
    class FakeHub:
        pass

    monkeypatch.setattr(ext_database, "Hub", FakeHub)
    monkeypatch.setattr(ext_database, "getcurrent", lambda: object())

    assert not ext_database._is_gevent_hub_callback()


def test_safe_pool_reset_defers_rollback_from_gevent_hub(monkeypatch):
    rollback = Mock()
    scheduled = []
    monkeypatch.setattr(ext_database, "_is_gevent_hub_callback", lambda: True)
    monkeypatch.setattr(ext_database, "spawn_later", lambda delay, func: scheduled.append((delay, func)))

    ext_database._safe_pool_reset(SimpleNamespace(rollback=rollback), SimpleNamespace(terminate_only=False))

    rollback.assert_not_called()
    assert scheduled == [(0, scheduled[0][1])]


def test_safe_pool_reset_rolls_back_immediately_outside_gevent_hub(monkeypatch):
    rollback = Mock()
    monkeypatch.setattr(ext_database, "_is_gevent_hub_callback", lambda: False)

    ext_database._safe_pool_reset(SimpleNamespace(rollback=rollback), SimpleNamespace(terminate_only=False))

    rollback.assert_called_once_with()
