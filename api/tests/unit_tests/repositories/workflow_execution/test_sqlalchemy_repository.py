from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import OperationalError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from core.repositories.sqlalchemy_workflow_execution_repository import SQLAlchemyWorkflowExecutionRepository
from core.workflow.entities.workflow_execution import WorkflowExecution, WorkflowExecutionStatus, WorkflowType
from models.account import Account, Tenant
from models.enums import WorkflowRunTriggeredFrom


@pytest.fixture
def mock_user():
    user = Account()
    user.id = "test-user-id"

    tenant = Tenant()
    tenant.id = "test-tenant"
    tenant.name = "Test Workspace"
    user._current_tenant = tenant

    return user


@pytest.fixture
def workflow_execution():
    return WorkflowExecution(
        id_="test-workflow-run-id",
        workflow_id="test-workflow-id",
        workflow_type=WorkflowType.CHAT,
        workflow_version="1.0",
        graph={"nodes": [], "edges": []},
        inputs={"query": "test"},
        outputs={},
        status=WorkflowExecutionStatus.SUCCEEDED,
        total_tokens=0,
        total_steps=1,
        started_at=datetime.now(UTC).replace(tzinfo=None),
        finished_at=datetime.now(UTC).replace(tzinfo=None),
    )


def _repository(session_factory, mock_user):
    return SQLAlchemyWorkflowExecutionRepository(
        session_factory=session_factory,
        user=mock_user,
        app_id="test-app",
        triggered_from=WorkflowRunTriggeredFrom.APP_RUN,
    )


def _session():
    session = MagicMock(spec=Session)
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=None)
    return session


def test_save_retries_transient_commit_error(mock_user, workflow_execution, monkeypatch):
    first_session = _session()
    second_session = _session()
    first_session.commit.side_effect = OperationalError("UPDATE workflow_runs", {}, RuntimeError("db restarting"))

    session_factory = MagicMock(spec=sessionmaker)
    session_factory.side_effect = [first_session, second_session]
    monkeypatch.setattr("core.repositories.sqlalchemy_retry._sleep_before_retry", MagicMock())

    repository = _repository(session_factory, mock_user)

    repository.save(workflow_execution)

    assert session_factory.call_count == 2
    first_session.merge.assert_called_once()
    first_session.commit.assert_called_once()
    second_session.merge.assert_called_once()
    second_session.commit.assert_called_once()


def test_save_does_not_retry_non_transient_error(mock_user, workflow_execution):
    session = _session()
    session.commit.side_effect = ValueError("invalid workflow status")

    session_factory = MagicMock(spec=sessionmaker)
    session_factory.return_value = session
    repository = _repository(session_factory, mock_user)

    with pytest.raises(ValueError, match="invalid workflow status"):
        repository.save(workflow_execution)

    session_factory.assert_called_once()
    session.commit.assert_called_once()


def test_save_logs_error_for_db_exception(mock_user, workflow_execution, caplog):
    session = _session()
    session.commit.side_effect = SQLAlchemyError("db constraint failed")

    session_factory = MagicMock(spec=sessionmaker)
    session_factory.return_value = session
    repository = _repository(session_factory, mock_user)

    with pytest.raises(SQLAlchemyError, match="db constraint failed"):
        repository.save(workflow_execution)

    assert session_factory.call_count == 1
    assert "Workflow execution persistence error" in caplog.text
    assert "test-workflow-run-id" in caplog.text
