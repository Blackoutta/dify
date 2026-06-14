import json
from unittest.mock import patch

import yaml
from click.testing import CliRunner
from flask import Flask

from commands.trace_config import trace_config
from extensions.ext_commands import init_app
from services.trace_config_batch_service import AppBatchResult, BatchResult


def successful_batch_result():
    return BatchResult(
        provider="langfuse",
        enabled=True,
        validation_skipped=False,
        results=[AppBatchResult(app_id="app-1", app_name="App One", status="created, enabled", enabled=True)],
    )


def test_providers_command_lists_supported_providers():
    result = CliRunner().invoke(trace_config, ["providers"])

    assert result.exit_code == 0
    assert result.output.splitlines() == ["aliyun", "arize", "langfuse", "langsmith", "opik", "phoenix", "weave"]


def test_template_command_outputs_json_by_default():
    result = CliRunner().invoke(trace_config, ["template", "langfuse"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["provider"] == "langfuse"
    assert payload["tracing_config"]["public_key"] == "<required>"
    assert payload["tracing_config"]["host"] == "https://api.langfuse.com"


def test_template_command_outputs_yaml_when_requested():
    result = CliRunner().invoke(trace_config, ["template", "langfuse", "--format", "yaml"])

    assert result.exit_code == 0
    payload = yaml.safe_load(result.output)
    assert payload["provider"] == "langfuse"
    assert payload["tracing_config"]["secret_key"] == "<required>"


def test_template_all_outputs_each_provider():
    result = CliRunner().invoke(trace_config, ["template", "--all"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert sorted(payload.keys()) == ["aliyun", "arize", "langfuse", "langsmith", "opik", "phoenix", "weave"]


def test_batch_command_accepts_inline_json_config():
    with patch(
        "commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()
    ) as batch_upsert:
        result = CliRunner().invoke(
            trace_config,
            [
                "batch",
                "--provider",
                "langfuse",
                "--app-ids",
                "app-1,app-2",
                "--config-json",
                '{"public_key":"pk","secret_key":"sk"}',
            ],
        )

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1", "app-2"],
        tracing_config={"public_key": "pk", "secret_key": "sk"},
        enable=True,
        validate=True,
        fail_fast=False,
    )
    assert "Provider: langfuse" in result.output
    assert "app-1" in result.output
    assert "Summary: total=1 succeeded=1 failed=0" in result.output


def test_batch_command_accepts_yaml_file(tmp_path):
    config_file = tmp_path / "trace-config.yaml"
    config_file.write_text(
        "provider: langfuse\napp_ids:\n  - app-1\ntracing_config:\n  public_key: pk\n  secret_key: sk\nenable: false\n",
        encoding="utf-8",
    )

    with patch(
        "commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()
    ) as batch_upsert:
        result = CliRunner().invoke(trace_config, ["batch", "--file", str(config_file), "--skip-validate"])

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1"],
        tracing_config={"public_key": "pk", "secret_key": "sk"},
        enable=False,
        validate=False,
        fail_fast=False,
    )


def test_batch_command_exits_one_when_any_app_fails():
    failed_result = BatchResult(
        provider="langfuse",
        enabled=True,
        validation_skipped=False,
        results=[AppBatchResult(app_id="missing", status="failed", error="App not found")],
    )

    with patch("commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=failed_result):
        result = CliRunner().invoke(
            trace_config,
            [
                "batch",
                "--provider",
                "langfuse",
                "--app-ids",
                "missing",
                "--config-json",
                '{"public_key":"pk","secret_key":"sk"}',
            ],
        )

    assert result.exit_code == 1
    assert "failed: App not found" in result.output


def test_batch_command_rejects_invalid_json():
    result = CliRunner().invoke(
        trace_config,
        ["batch", "--provider", "langfuse", "--app-ids", "app-1", "--config-json", "not-json"],
    )

    assert result.exit_code != 0
    assert "Invalid JSON" in result.output


def test_wizard_collects_manual_app_ids_and_runs_batch():
    user_input = "\n".join(
        [
            "langfuse",
            "2",
            "app-1,app-2",
            "pk",
            "sk",
            "",
            "y",
            "n",
            "y",
            "",
        ]
    )

    with patch(
        "commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()
    ) as batch_upsert:
        result = CliRunner().invoke(trace_config, ["wizard"], input=user_input)

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1", "app-2"],
        tracing_config={"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        enable=True,
        validate=False,
        fail_fast=False,
    )
    assert "Supported providers:" in result.output
    assert "App selection mode:" in result.output
    assert "Provider: langfuse" in result.output


def test_wizard_selects_apps_from_workspace():
    user_input = "\n".join(
        [
            "langfuse",
            "1",
            "1",
            "1,2",
            "pk",
            "sk",
            "",
            "y",
            "n",
            "y",
            "",
        ]
    )

    with (
        patch(
            "commands.trace_config.TraceConfigBatchService.list_workspaces",
            return_value=[{"id": "tenant-1", "name": "Workspace One"}],
        ),
        patch(
            "commands.trace_config.TraceConfigBatchService.list_apps_for_workspace",
            return_value=[
                {"id": "app-1", "name": "App One", "mode": "chat"},
                {"id": "app-2", "name": "App Two", "mode": "workflow"},
            ],
        ),
        patch(
            "commands.trace_config.TraceConfigBatchService.batch_upsert", return_value=successful_batch_result()
        ) as batch_upsert,
    ):
        result = CliRunner().invoke(trace_config, ["wizard"], input=user_input)

    assert result.exit_code == 0
    batch_upsert.assert_called_once_with(
        provider="langfuse",
        app_ids=["app-1", "app-2"],
        tracing_config={"public_key": "pk", "secret_key": "sk", "host": "https://api.langfuse.com"},
        enable=True,
        validate=False,
        fail_fast=False,
    )
    assert "1. Workspace One (tenant-1)" in result.output
    assert "1. App One [chat] (app-1)" in result.output
    assert "Apps (comma-separated numbers)" in result.output


def test_wizard_rejects_invalid_workspace_number():
    user_input = "\n".join(["langfuse", "1", "2", ""])

    with patch(
        "commands.trace_config.TraceConfigBatchService.list_workspaces",
        return_value=[{"id": "tenant-1", "name": "Workspace One"}],
    ):
        result = CliRunner().invoke(trace_config, ["wizard"], input=user_input)

    assert result.exit_code != 0
    assert "Invalid selection: 2" in result.output


def test_wizard_rejects_invalid_app_number():
    user_input = "\n".join(["langfuse", "1", "1", "2", ""])

    with (
        patch(
            "commands.trace_config.TraceConfigBatchService.list_workspaces",
            return_value=[{"id": "tenant-1", "name": "Workspace One"}],
        ),
        patch(
            "commands.trace_config.TraceConfigBatchService.list_apps_for_workspace",
            return_value=[{"id": "app-1", "name": "App One", "mode": "chat"}],
        ),
    ):
        result = CliRunner().invoke(trace_config, ["wizard"], input=user_input)

    assert result.exit_code != 0
    assert "Invalid selection: 2" in result.output


def test_trace_config_command_is_registered_on_flask_app():
    flask_app = Flask(__name__)

    init_app(flask_app)

    assert "trace-config" in flask_app.cli.commands
