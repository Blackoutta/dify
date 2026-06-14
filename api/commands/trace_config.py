from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import click
import yaml

from services.trace_config_batch_service import BatchResult, TraceConfigBatchError, TraceConfigBatchService


def _dump_payload(payload: Any, output_format: str) -> str:
    if output_format == "yaml":
        return yaml.safe_dump(payload, sort_keys=False, allow_unicode=True)
    return json.dumps(payload, indent=2, sort_keys=False)


def _parse_app_ids(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(",") if part.strip()]


def _load_config_file(path: str) -> dict[str, Any]:
    file_path = Path(path)
    try:
        raw_content = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise click.ClickException(f"Unable to read config file: {exc}") from exc

    try:
        if file_path.suffix.lower() == ".json":
            payload = json.loads(raw_content)
        else:
            payload = yaml.safe_load(raw_content)
    except (json.JSONDecodeError, yaml.YAMLError) as exc:
        raise click.ClickException(f"Invalid JSON/YAML file: {exc}") from exc

    if not isinstance(payload, dict):
        raise click.ClickException("Config file must contain an object.")
    return payload


def _load_inline_json(value: str) -> dict[str, Any]:
    try:
        payload = json.loads(value)
    except json.JSONDecodeError as exc:
        raise click.ClickException(f"Invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise click.ClickException("--config-json must contain a JSON object.")
    return payload


def _print_batch_result(result: BatchResult):
    click.echo(f"Provider: {result.provider}")
    click.echo(f"Enable tracing: {str(result.enabled).lower()}")
    click.echo(f"External validation skipped: {str(result.validation_skipped).lower()}")
    for app_result in result.results:
        name = f" ({app_result.app_name})" if app_result.app_name else ""
        if app_result.error:
            click.echo(f"- {app_result.app_id}{name}: failed: {app_result.error}")
        else:
            click.echo(f"- {app_result.app_id}{name}: {app_result.status}")
    click.echo(f"Summary: total={result.total} succeeded={result.succeeded} failed={result.failed}")


@click.group("trace-config")
def trace_config():
    """Manage trace provider configuration in batch."""


@trace_config.command("providers")
def providers_command():
    """List supported trace providers."""
    for provider in TraceConfigBatchService.list_providers():
        click.echo(provider)


@trace_config.command("template")
@click.argument("provider", required=False)
@click.option("--all", "include_all", is_flag=True, help="Print templates for all supported providers.")
@click.option("--format", "output_format", type=click.Choice(["json", "yaml"]), default="json", show_default=True)
def template_command(provider: str | None, include_all: bool, output_format: str):
    """Print a trace provider input template."""
    try:
        if include_all:
            payload = TraceConfigBatchService.get_all_templates()
        elif provider:
            payload = TraceConfigBatchService.get_template(provider)
        else:
            raise click.UsageError("Provide a provider or use --all.")
    except TraceConfigBatchError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(_dump_payload(payload, output_format))


@trace_config.command("batch")
@click.option("--provider", help="Trace provider name.")
@click.option("--app-ids", help="Comma-separated app IDs.")
@click.option("--config-json", help="Provider tracing_config as a JSON object.")
@click.option(
    "--file", "config_file", help="JSON or YAML file containing provider, app_ids, tracing_config, and enable."
)
@click.option("--no-enable", is_flag=True, help="Only upsert credentials/config; do not enable app tracing.")
@click.option("--skip-validate", is_flag=True, help="Skip external provider credential validation.")
@click.option("--fail-fast", is_flag=True, help="Stop after the first per-app failure.")
def batch_command(
    provider: str | None,
    app_ids: str | None,
    config_json: str | None,
    config_file: str | None,
    no_enable: bool,
    skip_validate: bool,
    fail_fast: bool,
):
    """Batch upsert trace provider configuration for explicit app IDs."""
    if config_file and (provider or app_ids or config_json):
        raise click.UsageError("Use either --file or inline --provider/--app-ids/--config-json input, not both.")

    if config_file:
        payload = _load_config_file(config_file)
        resolved_provider = payload.get("provider")
        resolved_app_ids = payload.get("app_ids") or []
        tracing_config = payload.get("tracing_config")
        enable = bool(payload.get("enable", True))
    else:
        if not provider or not app_ids or not config_json:
            raise click.UsageError("Inline mode requires --provider, --app-ids, and --config-json.")
        resolved_provider = provider
        resolved_app_ids = _parse_app_ids(app_ids)
        tracing_config = _load_inline_json(config_json)
        enable = True

    if no_enable:
        enable = False
    if not isinstance(resolved_provider, str) or not resolved_provider.strip():
        raise click.UsageError("Provider is required.")
    if not isinstance(resolved_app_ids, list) or not all(isinstance(app_id, str) for app_id in resolved_app_ids):
        raise click.UsageError("app_ids must be a list of strings.")
    if not isinstance(tracing_config, dict):
        raise click.UsageError("tracing_config must be an object.")

    try:
        result = TraceConfigBatchService.batch_upsert(
            provider=resolved_provider,
            app_ids=[app_id.strip() for app_id in resolved_app_ids if app_id.strip()],
            tracing_config=tracing_config,
            enable=enable,
            validate=not skip_validate,
            fail_fast=fail_fast,
        )
    except TraceConfigBatchError as exc:
        raise click.ClickException(str(exc)) from exc

    _print_batch_result(result)
    if result.has_failures:
        raise click.exceptions.Exit(1)


def _choose_numbered_option(options: list[dict[str, str]], prompt: str) -> dict[str, str]:
    raw_value = click.prompt(prompt)
    try:
        index = int(raw_value) - 1
    except ValueError as exc:
        raise click.ClickException(f"Invalid selection: {raw_value}") from exc
    if index < 0 or index >= len(options):
        raise click.ClickException(f"Invalid selection: {raw_value}")
    return options[index]


def _choose_numbered_options(options: list[dict[str, str]], prompt: str) -> list[dict[str, str]]:
    raw_value = click.prompt(prompt)
    selected: list[dict[str, str]] = []
    for part in raw_value.split(","):
        value = part.strip()
        try:
            index = int(value) - 1
        except ValueError as exc:
            raise click.ClickException(f"Invalid selection: {value}") from exc
        if index < 0 or index >= len(options):
            raise click.ClickException(f"Invalid selection: {value}")
        selected.append(options[index])
    return selected


def _collect_app_ids_for_wizard() -> list[str]:
    click.echo("App selection mode:")
    click.echo("1. Select from workspace")
    click.echo("2. Enter app IDs manually")
    mode = click.prompt("Mode", type=click.Choice(["1", "2"]))
    if mode == "2":
        return _parse_app_ids(click.prompt("App IDs (comma-separated)"))

    workspaces = TraceConfigBatchService.list_workspaces()
    if not workspaces:
        raise click.ClickException("No workspaces found.")
    for idx, workspace in enumerate(workspaces, start=1):
        click.echo(f"{idx}. {workspace['name']} ({workspace['id']})")
    workspace = _choose_numbered_option(workspaces, "Workspace")

    apps = TraceConfigBatchService.list_apps_for_workspace(workspace["id"])
    if not apps:
        raise click.ClickException(f"No apps found in workspace {workspace['name']}.")
    for idx, app in enumerate(apps, start=1):
        click.echo(f"{idx}. {app['name']} [{app['mode']}] ({app['id']})")
    selected_apps = _choose_numbered_options(apps, "Apps (comma-separated numbers)")
    return [app["id"] for app in selected_apps]


def _collect_config_from_template(provider: str) -> dict[str, Any]:
    template = TraceConfigBatchService.get_template(provider)
    config: dict[str, Any] = {}
    for key, default_value in template["tracing_config"].items():
        if default_value == "<required>":
            config[key] = click.prompt(key, hide_input="key" in key.lower() or "secret" in key.lower())
        else:
            config[key] = click.prompt(key, default=str(default_value), show_default=True)
    return config


@trace_config.command("wizard")
def wizard_command():
    """Interactively configure trace provider settings for multiple apps."""
    providers = TraceConfigBatchService.list_providers()
    click.echo("Supported providers:")
    for provider_name in providers:
        click.echo(f"- {provider_name}")

    provider = click.prompt("Provider", type=click.Choice(providers))
    app_ids = _collect_app_ids_for_wizard()
    tracing_config = _collect_config_from_template(provider)
    enable = click.confirm("Enable tracing after credentials are written?", default=True)
    validate = click.confirm("Validate credentials with provider API?", default=True)

    click.echo("Summary:")
    click.echo(f"Provider: {provider}")
    click.echo(f"App IDs: {', '.join(app_ids)}")
    click.echo(f"Enable tracing: {str(enable).lower()}")
    click.echo(f"External validation: {str(validate).lower()}")
    if not click.confirm("Proceed?", default=False):
        click.echo("Aborted.")
        return

    try:
        result = TraceConfigBatchService.batch_upsert(
            provider=provider,
            app_ids=app_ids,
            tracing_config=tracing_config,
            enable=enable,
            validate=validate,
            fail_fast=False,
        )
    except TraceConfigBatchError as exc:
        raise click.ClickException(str(exc)) from exc

    _print_batch_result(result)
    if result.has_failures:
        raise click.exceptions.Exit(1)
