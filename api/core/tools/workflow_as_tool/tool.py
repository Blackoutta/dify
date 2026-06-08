import json
import logging
from collections.abc import Generator
from typing import Any, Optional, cast

from flask_login import current_user
from sqlalchemy import select

from core.db.session_factory import session_factory
from core.file import FILE_MODEL_IDENTITY, File, FileTransferMethod
from core.ops.trace_context import ParentTraceContext, extract_parent_trace_context_from_args
from core.tools.__base.tool import Tool
from core.tools.__base.tool_runtime import ToolRuntime
from core.tools.entities.tool_entities import (
    ToolEntity,
    ToolInvokeMessage,
    ToolParameter,
    ToolProviderType,
)
from core.tools.errors import ToolInvokeError
from factories.file_factory import build_from_mapping
from models.account import Account
from models.model import App, EndUser
from models.workflow import Workflow

logger = logging.getLogger(__name__)


class WorkflowTool(Tool):
    workflow_app_id: str
    version: str
    workflow_entities: dict[str, Any]
    workflow_call_depth: int
    thread_pool_id: Optional[str] = None
    workflow_as_tool_id: str
    _parent_trace_context: ParentTraceContext | None
    _trace_session_id: str | None

    label: str

    """
    Workflow tool.
    """

    def __init__(
        self,
        workflow_app_id: str,
        workflow_as_tool_id: str,
        version: str,
        workflow_entities: dict[str, Any],
        workflow_call_depth: int,
        entity: ToolEntity,
        runtime: ToolRuntime,
        label: str = "Workflow",
        thread_pool_id: Optional[str] = None,
    ):
        self.workflow_app_id = workflow_app_id
        self.workflow_as_tool_id = workflow_as_tool_id
        self.version = version
        self.workflow_entities = workflow_entities
        self.workflow_call_depth = workflow_call_depth
        self.thread_pool_id = thread_pool_id
        self.label = label
        self._parent_trace_context = None
        self._trace_session_id = None

        super().__init__(entity=entity, runtime=runtime)

    def tool_provider_type(self) -> ToolProviderType:
        """
        get the tool provider type

        :return: the tool provider type
        """
        return ToolProviderType.WORKFLOW

    def _invoke(
        self,
        user_id: str,
        tool_parameters: dict[str, Any],
        conversation_id: Optional[str] = None,
        app_id: Optional[str] = None,
        message_id: Optional[str] = None,
    ) -> Generator[ToolInvokeMessage, None, None]:
        """
        invoke the tool
        """
        app, workflow = self._resolve_app_and_workflow()

        # transform the tool parameters
        tool_parameters, files = self._transform_args(tool_parameters=tool_parameters)

        from core.app.apps.workflow.app_generator import WorkflowAppGenerator

        generator = WorkflowAppGenerator()
        assert self.runtime is not None
        assert self.runtime.invoke_from is not None

        generator_args: dict[str, Any] = {"inputs": tool_parameters, "files": files}
        if self._parent_trace_context is not None:
            generator_args.update(
                extract_parent_trace_context_from_args({"parent_trace_context": self._parent_trace_context})
            )
        if self._trace_session_id:
            generator_args["trace_session_id"] = self._trace_session_id

        result = generator.generate(
            app_model=app,
            workflow=workflow,
            user=cast("Account | EndUser", current_user),
            args=generator_args,
            invoke_from=self.runtime.invoke_from,
            streaming=False,
            call_depth=self.workflow_call_depth + 1,
            workflow_thread_pool_id=self.thread_pool_id,
        )
        assert isinstance(result, dict)
        data = result.get("data", {})

        if err := data.get("error"):
            raise ToolInvokeError(err)

        outputs = data.get("outputs")
        if outputs is None:
            outputs = {}
        else:
            outputs, files = self._extract_files(outputs)  # type: ignore
            for file in files:
                yield self.create_file_message(file)  # type: ignore

        yield self.create_text_message(json.dumps(outputs, ensure_ascii=False))
        yield self.create_json_message(outputs)

    def _resolve_app_and_workflow(self) -> tuple[App, Workflow]:
        app_key_present = "app" in self.workflow_entities
        workflow_key_present = "workflow" in self.workflow_entities

        app = self.workflow_entities.get("app")
        workflow = self.workflow_entities.get("workflow")

        if app_key_present and not isinstance(app, App):
            raise ValueError("invalid cached workflow tool app")
        if workflow_key_present and not isinstance(workflow, Workflow):
            raise ValueError("invalid cached workflow tool workflow")

        if not app_key_present:
            app = self._get_app(app_id=self.workflow_app_id)
        if not workflow_key_present:
            workflow = self._get_workflow(app_id=self.workflow_app_id, version=self.version)

        assert isinstance(app, App)
        assert isinstance(workflow, Workflow)
        return app, workflow

    def fork_tool_runtime(self, runtime: ToolRuntime) -> "WorkflowTool":
        """
        fork a new tool with metadata

        :return: the new tool
        """
        forked = self.__class__(
            entity=self.entity.model_copy(deep=True),
            runtime=runtime,
            workflow_app_id=self.workflow_app_id,
            workflow_as_tool_id=self.workflow_as_tool_id,
            workflow_entities=dict(self.workflow_entities),
            workflow_call_depth=self.workflow_call_depth,
            version=self.version,
            label=self.label,
        )
        forked._parent_trace_context = self._parent_trace_context.model_copy() if self._parent_trace_context else None
        forked._trace_session_id = self._trace_session_id
        return forked

    def set_parent_trace_context(
        self,
        *,
        parent_workflow_run_id: str,
        parent_node_execution_id: str,
    ) -> None:
        self._parent_trace_context = ParentTraceContext(
            parent_workflow_run_id=parent_workflow_run_id,
            parent_node_execution_id=parent_node_execution_id,
        )

    def clear_parent_trace_context(self) -> None:
        self._parent_trace_context = None

    def set_trace_session_id(self, trace_session_id: str | None) -> None:
        self._trace_session_id = trace_session_id

    def _get_workflow(self, app_id: str, version: str) -> Workflow:
        """
        get the workflow by app id and version
        """
        with session_factory.create_session() as session, session.begin():
            if not version:
                workflow = (
                    session.query(Workflow)
                    .filter(Workflow.app_id == app_id, Workflow.version != "draft")
                    .order_by(Workflow.created_at.desc())
                    .first()
                )
            else:
                stmt = select(Workflow).where(Workflow.app_id == app_id, Workflow.version == version)
                workflow = session.scalar(stmt)

            if not workflow:
                raise ValueError("workflow not found or not published")

            session.expunge(workflow)
            return workflow

    def _get_app(self, app_id: str) -> App:
        """
        get the app by app id
        """
        stmt = select(App).where(App.id == app_id)
        with session_factory.create_session() as session, session.begin():
            app = session.scalar(stmt)
            if not app:
                raise ValueError("app not found")

            session.expunge(app)
            return app

    def _transform_args(self, tool_parameters: dict) -> tuple[dict, list[dict]]:
        """
        transform the tool parameters

        :param tool_parameters: the tool parameters
        :return: tool_parameters, files
        """
        parameter_rules = self.get_merged_runtime_parameters()
        parameters_result = {}
        files = []
        for parameter in parameter_rules:
            if parameter.type == ToolParameter.ToolParameterType.SYSTEM_FILES:
                file = tool_parameters.get(parameter.name)
                if file:
                    try:
                        file_var_list = [File.model_validate(f) for f in file]
                        for file in file_var_list:
                            file_dict: dict[str, str | None] = {
                                "transfer_method": file.transfer_method.value,
                                "type": file.type.value,
                            }
                            if file.transfer_method == FileTransferMethod.TOOL_FILE:
                                file_dict["tool_file_id"] = file.related_id
                            elif file.transfer_method == FileTransferMethod.LOCAL_FILE:
                                file_dict["upload_file_id"] = file.related_id
                            elif file.transfer_method == FileTransferMethod.REMOTE_URL:
                                file_dict["url"] = file.generate_url()

                            files.append(file_dict)
                    except Exception:
                        logger.exception(f"Failed to transform file {file}")
            else:
                parameters_result[parameter.name] = tool_parameters.get(parameter.name)

        return parameters_result, files

    def _extract_files(self, outputs: dict) -> tuple[dict, list[File]]:
        """
        extract files from the result

        :return: the result, files
        """
        files: list[File] = []
        result = {}
        for key, value in outputs.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get("dify_model_identity") == FILE_MODEL_IDENTITY:
                        item = self._update_file_mapping(item)
                        file = build_from_mapping(
                            mapping=item,
                            tenant_id=str(cast(ToolRuntime, self.runtime).tenant_id),
                        )
                        files.append(file)
            elif isinstance(value, dict) and value.get("dify_model_identity") == FILE_MODEL_IDENTITY:
                value = self._update_file_mapping(value)
                file = build_from_mapping(
                    mapping=value,
                    tenant_id=str(cast(ToolRuntime, self.runtime).tenant_id),
                )
                files.append(file)

            result[key] = value

        return result, files

    def _update_file_mapping(self, file_dict: dict) -> dict:
        transfer_method = FileTransferMethod.value_of(file_dict.get("transfer_method"))
        if transfer_method == FileTransferMethod.TOOL_FILE:
            file_dict["tool_file_id"] = file_dict.get("related_id")
        elif transfer_method == FileTransferMethod.LOCAL_FILE:
            file_dict["upload_file_id"] = file_dict.get("related_id")
        return file_dict
