from sqlalchemy import create_engine

from core.repositories.factory import DifyCoreRepositoryFactory
from core.repositories.sqlalchemy_workflow_node_execution_repository import SQLAlchemyWorkflowNodeExecutionRepository
from core.repositories.workflow_node_execution_activemq_repository import ActiveMQWorkflowNodeExecutionRepository
from models import Account, Tenant
from models.enums import WorkflowRunTriggeredFrom
from models.workflow import WorkflowNodeExecutionTriggeredFrom


def _account() -> Account:
    account = Account(name="Test", email="test@example.com")
    account.id = "user-id"
    tenant = Tenant(name="Tenant")
    tenant.id = "tenant-id"
    account._current_tenant = tenant
    return account


def _engine():
    return create_engine("sqlite:///:memory:")


def test_factory_uses_sqlalchemy_when_async_disabled(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", False)

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.APP_RUN,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_uses_activemq_for_app_workflow_runs(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.APP_RUN,
    )

    assert isinstance(repo, ActiveMQWorkflowNodeExecutionRepository)


def test_factory_keeps_debugging_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.DEBUGGING,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_keeps_single_step_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.SINGLE_STEP,
        workflow_triggered_from=WorkflowRunTriggeredFrom.DEBUGGING,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_keeps_rag_pipeline_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.RAG_PIPELINE_RUN,
        workflow_triggered_from=WorkflowRunTriggeredFrom.RAG_PIPELINE_RUN,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)


def test_factory_keeps_missing_workflow_trigger_synchronous(monkeypatch) -> None:
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_ASYNC_ENABLED", True)
    monkeypatch.setattr("core.repositories.factory.dify_config.WORKFLOW_LOG_QUEUE_PROVIDER", "activemq")

    repo = DifyCoreRepositoryFactory.create_workflow_node_execution_repository(
        session_factory=_engine(),
        user=_account(),
        app_id="app-id",
        triggered_from=WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN,
    )

    assert isinstance(repo, SQLAlchemyWorkflowNodeExecutionRepository)
