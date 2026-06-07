from datetime import datetime
from types import SimpleNamespace

from core.app.apps.workflow.generate_task_pipeline import WorkflowAppGenerateTaskPipeline
from core.app.entities.app_invoke_entities import InvokeFrom
from core.workflow.entities.workflow_execution import WorkflowExecution, WorkflowExecutionStatus, WorkflowType
from models.enums import CreatorUserRole
from models.workflow import WorkflowAppLogCreatedFrom


class FakeSession:
    def __init__(self):
        self.added = []
        self.commits = 0

    def scalar(self, _statement):
        raise AssertionError("workflow app log should not require a persisted WorkflowRun")

    def add(self, obj):
        self.added.append(obj)

    def commit(self):
        self.commits += 1


def test_save_workflow_app_log_does_not_require_persisted_workflow_run():
    pipeline = WorkflowAppGenerateTaskPipeline.__new__(WorkflowAppGenerateTaskPipeline)
    pipeline._application_generate_entity = SimpleNamespace(
        invoke_from=InvokeFrom.SERVICE_API,
        app_config=SimpleNamespace(
            tenant_id="tenant-1",
            app_id="app-1",
        ),
    )
    pipeline._created_by_role = CreatorUserRole.ACCOUNT
    pipeline._user_id = "user-1"

    session = FakeSession()
    workflow_execution = WorkflowExecution(
        id_="run-1",
        workflow_id="workflow-1",
        workflow_type=WorkflowType.WORKFLOW,
        workflow_version="1",
        graph={},
        inputs={},
        outputs={},
        status=WorkflowExecutionStatus.SUCCEEDED,
        started_at=datetime(2026, 1, 1),
    )

    pipeline._save_workflow_app_log(session=session, workflow_execution=workflow_execution)

    assert len(session.added) == 1
    workflow_app_log = session.added[0]
    assert workflow_app_log.tenant_id == "tenant-1"
    assert workflow_app_log.app_id == "app-1"
    assert workflow_app_log.workflow_id == "workflow-1"
    assert workflow_app_log.workflow_run_id == "run-1"
    assert workflow_app_log.created_from == WorkflowAppLogCreatedFrom.SERVICE_API.value
    assert workflow_app_log.created_by_role == CreatorUserRole.ACCOUNT
    assert workflow_app_log.created_by == "user-1"
    assert session.commits == 1
