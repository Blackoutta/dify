from unittest.mock import MagicMock

import services.app_generate_service as ags_module
from core.app.entities.app_invoke_entities import InvokeFrom
from models.model import AppMode
from services.app_generate_service import AppGenerateService


class _DummyRateLimit:
    @staticmethod
    def gen_request_key() -> str:
        return "dummy-request-id"

    def __init__(self, client_id: str, max_active_requests: int) -> None:
        self.client_id = client_id
        self.max_active_requests = max_active_requests

    def enter(self, request_id: str | None = None) -> str:
        return request_id or "dummy-request-id"

    def exit(self, request_id: str) -> None:
        return None

    def generate(self, generator, request_id: str):
        return generator


def test_advanced_chat_blocking_injects_pause_state_config(monkeypatch):
    monkeypatch.setattr(ags_module.dify_config, "BILLING_ENABLED", False)
    monkeypatch.setattr(ags_module, "RateLimit", _DummyRateLimit)

    workflow = MagicMock()
    workflow.created_by = "owner-id"
    monkeypatch.setattr(AppGenerateService, "_get_workflow", lambda *args, **kwargs: workflow)
    monkeypatch.setattr(ags_module.session_factory, "get_session_maker", lambda: "session-maker")

    generator_instance = MagicMock()
    generator_instance.generate.return_value = {"result": "advanced-blocking"}
    generator_instance.convert_to_event_stream.side_effect = lambda payload: payload
    monkeypatch.setattr(ags_module, "AdvancedChatAppGenerator", lambda: generator_instance)

    app_model = MagicMock()
    app_model.mode = AppMode.ADVANCED_CHAT
    app_model.id = "app-id"
    app_model.tenant_id = "tenant-id"
    app_model.max_active_requests = 0
    app_model.is_agent = False

    user = MagicMock()
    user.id = "user-id"

    result = AppGenerateService.generate(
        app_model=app_model,
        user=user,
        args={"workflow_id": None, "query": "hi", "inputs": {}},
        invoke_from=InvokeFrom.SERVICE_API,
        streaming=False,
    )

    assert result == {"result": "advanced-blocking"}
    call_kwargs = generator_instance.generate.call_args.kwargs
    assert call_kwargs["streaming"] is False
    assert call_kwargs["pause_state_config"] is not None
    assert call_kwargs["pause_state_config"].session_factory == "session-maker"
    assert call_kwargs["pause_state_config"].state_owner_user_id == "owner-id"
