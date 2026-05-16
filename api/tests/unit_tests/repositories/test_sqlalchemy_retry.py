from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import DBAPIError, OperationalError, SQLAlchemyError

from core.repositories.sqlalchemy_retry import (
    SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS,
    execute_with_db_retry,
    is_retryable_db_error,
)


def _session():
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=None)
    return session


def test_is_retryable_db_error_accepts_operational_error():
    exc = OperationalError("SELECT 1", {}, RuntimeError("db restarting"))

    assert is_retryable_db_error(exc)


def test_is_retryable_db_error_accepts_invalidated_dbapi_error():
    exc = DBAPIError(
        "SELECT 1",
        {},
        RuntimeError("connection invalidated"),
        connection_invalidated=True,
    )

    assert is_retryable_db_error(exc)


def test_is_retryable_db_error_rejects_generic_sqlalchemy_error():
    assert not is_retryable_db_error(SQLAlchemyError("constraint failed"))


def test_execute_with_db_retry_retries_with_new_session(monkeypatch):
    first_session = _session()
    second_session = _session()
    session_factory = MagicMock(side_effect=[first_session, second_session])
    logger = MagicMock()
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", MagicMock())

    operation = MagicMock(
        side_effect=[
            OperationalError("UPDATE workflow_runs", {}, RuntimeError("db restarting")),
            "ok",
        ]
    )

    result = execute_with_db_retry(
        session_factory=session_factory,
        operation=operation,
        logger=logger,
        operation_name="Workflow execution",
        context="execution_id test",
    )

    assert result == "ok"
    assert session_factory.call_count == 2
    operation.assert_any_call(first_session)
    operation.assert_any_call(second_session)
    first_session.rollback.assert_called_once()
    logger.exception.assert_called_once()
    logger.info.assert_called_once()


def test_execute_with_db_retry_reraises_after_retry_exhaustion(monkeypatch):
    sessions = [_session() for _ in range(SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS)]
    session_factory = MagicMock(side_effect=sessions)
    logger = MagicMock()
    sleep = MagicMock()
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", sleep)

    operation = MagicMock(
        side_effect=OperationalError("UPDATE workflow_runs", {}, RuntimeError("db restarting"))
    )

    with pytest.raises(OperationalError):
        execute_with_db_retry(
            session_factory=session_factory,
            operation=operation,
            logger=logger,
            operation_name="Workflow execution",
            context="execution_id test",
        )

    assert session_factory.call_count == SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS
    assert operation.call_count == SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS
    assert sleep.call_count == SQLALCHEMY_PERSISTENCE_MAX_ATTEMPTS - 1


def test_execute_with_db_retry_does_not_retry_generic_sqlalchemy_error(monkeypatch):
    session = _session()
    session_factory = MagicMock(return_value=session)
    logger = MagicMock()
    sleep = MagicMock()
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", sleep)

    operation = MagicMock(side_effect=SQLAlchemyError("constraint failed"))

    with pytest.raises(SQLAlchemyError, match="constraint failed"):
        execute_with_db_retry(
            session_factory=session_factory,
            operation=operation,
            logger=logger,
            operation_name="Workflow execution",
            context="execution_id test",
        )

    session_factory.assert_called_once()
    operation.assert_called_once_with(session)
    sleep.assert_not_called()


def test_execute_with_db_retry_retries_invalidated_dbapi_error(monkeypatch):
    first_session = _session()
    second_session = _session()
    session_factory = MagicMock(side_effect=[first_session, second_session])
    logger = MagicMock()
    sleep = MagicMock()
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", sleep)

    operation = MagicMock(
        side_effect=[
            DBAPIError(
                "UPDATE workflow_runs",
                {},
                RuntimeError("connection invalidated"),
                connection_invalidated=True,
            ),
            "ok",
        ]
    )

    result = execute_with_db_retry(
        session_factory=session_factory,
        operation=operation,
        logger=logger,
        operation_name="Workflow execution",
        context="execution_id test",
    )

    assert result == "ok"
    assert session_factory.call_count == 2
    operation.assert_any_call(first_session)
    operation.assert_any_call(second_session)
    sleep.assert_called_once_with(1)


def test_sleep_before_retry_uses_two_four_eight_second_backoff_with_subsecond_jitter(monkeypatch):
    sleep = MagicMock()
    monkeypatch.setattr("core.repositories.sqlalchemy_retry.random.uniform", MagicMock(return_value=0.5))
    monkeypatch.setattr("core.repositories.sqlalchemy_retry.time.sleep", sleep)

    from core.repositories.sqlalchemy_retry import _sleep_before_retry

    _sleep_before_retry(1)
    _sleep_before_retry(2)
    _sleep_before_retry(3)

    assert [call.args[0] for call in sleep.call_args_list] == [2.5, 4.5, 8.5]
