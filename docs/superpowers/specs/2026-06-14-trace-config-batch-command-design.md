# Trace Config Batch Flask Command Design

## Background

Dify currently exposes trace provider configuration through console APIs:

- `POST/PATCH /console/api/apps/<app_id>/trace-config` creates or updates provider credentials/config for one app.
- `POST /console/api/apps/<app_id>/trace` enables app tracing and selects the provider by writing `App.tracing`.

Ops and CI users need a Flask command that can configure one trace provider for multiple apps in one run. The command should support both interactive operation and deterministic automation.

## Goals

- Configure one supported trace provider for one or more explicit app IDs.
- Support a human-friendly wizard mode that can either accept app IDs manually or help users select apps from a selected workspace.
- Support a programmatic mode suitable for CI/CD.
- Provide templates that describe the required input schema for every supported provider.
- Default to idempotent upsert behavior.
- Default to enabling tracing after credentials are written, with an option to only store credentials.
- Keep new batch service and CLI support isolated in new files.

## Non-goals

- Supporting workspace, tenant, app mode, or name-pattern app selection in the non-interactive `batch` command.
- Supporting app mode or name-pattern filtering in `wizard`; wizard may list workspaces and apps only as an interactive convenience for selecting explicit app IDs.
- Adding new trace providers.
- Changing existing console API behavior.
- Reworking existing `OpsService` endpoints.

## Supported providers

The command uses the existing provider metadata from `TracingProviderEnum` and `provider_config_map`. The initial provider set is:

- `arize`
- `phoenix`
- `langfuse`
- `langsmith`
- `opik`
- `weave`
- `aliyun`

Provider templates should be generated dynamically from existing Pydantic config classes and provider metadata, not hand-maintained in the CLI.

## CLI structure

Add a new command group:

```bash
flask trace-config providers
flask trace-config template <provider>
flask trace-config template --all
flask trace-config batch ...
flask trace-config wizard
```

Use `trace-config` because it aligns with the existing `/trace-config` API path.

### `providers`

Lists supported provider names. This is useful for scripting and for humans discovering valid values.

Example:

```bash
flask trace-config providers
```

### `template`

Prints input templates for one provider or all providers.

Examples:

```bash
flask trace-config template langfuse
flask trace-config template --all
flask trace-config template langfuse --format json
flask trace-config template langfuse --format yaml
```

Template shape:

```json
{
  "provider": "langfuse",
  "app_ids": ["<app-id>"],
  "tracing_config": {
    "public_key": "<required>",
    "secret_key": "<required>",
    "host": "https://api.langfuse.com"
  },
  "enable": true
}
```

Required fields are marked as `"<required>"`. Fields with defaults use the default value from the Pydantic model.

### `batch`

Programmatic CI/CD mode. Supports inline CLI input and config-file input.

Inline example:

```bash
flask trace-config batch \
  --provider langfuse \
  --app-ids app-id-1,app-id-2 \
  --config-json '{"public_key":"pk-lf-xxx","secret_key":"sk-lf-xxx","host":"https://api.langfuse.com"}'
```

File example:

```bash
flask trace-config batch --file trace-config.yaml
flask trace-config batch --file trace-config.json
```

File schema:

```yaml
provider: langfuse
app_ids:
  - 5dfabab3-1911-47bd-91fb-512a412a436a
  - 11111111-1111-1111-1111-111111111111
tracing_config:
  public_key: pk-lf-xxx
  secret_key: sk-lf-xxx
  host: https://api.langfuse.com
enable: true
```

Options:

```bash
--no-enable       # only upsert credentials/config; do not enable app tracing
--skip-validate   # skip external provider api_check()
--fail-fast       # stop after the first per-app failure
```

Default behavior:

1. Validate the provider and tracing config.
2. Run provider credentials validation once for the shared config.
3. For every app ID, upsert the provider config.
4. Enable app tracing and select the provider.
5. Continue processing remaining app IDs after per-app failures.
6. Exit with code `1` if any app failed; otherwise exit with code `0`.

### `wizard`

Interactive mode for human ops users.

Flow:

1. Display supported providers.
2. Prompt for provider selection.
3. Prompt for app selection mode:
   - `Select from workspace`
   - `Enter app IDs manually`
4. If selecting from workspace:
   1. List available workspaces by stable number, workspace name, and workspace ID.
   2. Prompt for one workspace number.
   3. List apps in that workspace by stable number, app name, app mode, and app ID.
   4. Prompt for one or more app numbers, comma separated.
   5. Resolve the selected numbers to explicit app IDs.
5. If entering manually, prompt for one or more comma-separated app IDs.
6. Prompt for provider-specific fields. Optional/default fields can be accepted with Enter.
7. Ask whether to enable tracing. Default: yes.
8. Ask whether to validate credentials. Default: yes.
9. Resolve and display app ID, app name when available, provider, and enable summary.
10. Ask for final confirmation.
11. Execute the same batch service path as `batch`.
12. Print per-app results and summary.

The workspace/app picker is only an interactive helper. The execution boundary remains explicit app IDs passed into `TraceConfigBatchService.batch_upsert()`.

## File isolation

Add new files instead of expanding existing large modules:

```text
api/services/trace_config_batch_service.py
api/commands/trace_config.py
```

Registration should be done by importing and adding the command group in `api/extensions/ext_commands.py`.

`api/commands.py` should not absorb the batch service or wizard implementation. If a small compatibility import is needed, keep it minimal.

## Service design

Create a dedicated service, for example `TraceConfigBatchService`, in `api/services/trace_config_batch_service.py`.

Primary API:

```python
TraceConfigBatchService.batch_upsert(
    provider: str,
    app_ids: list[str],
    tracing_config: dict,
    enable: bool = True,
    validate: bool = True,
    fail_fast: bool = False,
) -> BatchResult
```

Responsibilities:

- Normalize and validate provider names.
- Generate provider templates.
- Validate tracing config against the provider Pydantic config class.
- Run external provider credential validation once when `validate=True`.
- Upsert `TraceAppConfig` for each app.
- Optionally update `App.tracing` with `{"enabled": true, "tracing_provider": provider}`.
- Return structured per-app results and summary counts.
- Provide read-only helpers for wizard app selection, such as listing workspaces and listing apps for one workspace. These helpers must not be used by the non-interactive `batch` command.

The service should reuse existing primitives where appropriate:

- `provider_config_map`
- provider config classes
- `OpsTraceManager.encrypt_tracing_config()`
- `OpsTraceManager.check_trace_config_is_effective()`
- `OpsTraceManager.update_app_tracing_config()` or equivalent isolated write logic
- `TraceAppConfig`, `App`, and `Tenant` models

Avoid moving existing API behavior in this feature. The current `OpsService` may remain as-is.

## Upsert behavior

For each app ID:

1. Load the app. If not found, mark that app as failed.
2. Look for an existing `TraceAppConfig` row matching app ID and provider.
3. If found, update encrypted config.
4. If not found, create a new row.
5. If `enable=True`, enable tracing for the app with the selected provider.
6. Commit the app's transaction.

The command is idempotent: running the same input repeatedly should end in the same app tracing state.

## Validation behavior

Validation has two levels:

1. Schema validation: always validate input against the provider config class before writing.
2. External provider validation: call provider `api_check()` once before the app loop unless `--skip-validate` is used.

If provider or schema validation fails, the whole batch fails before changing any app. If external validation fails, the whole batch fails before changing any app unless validation was skipped.

## Error handling and exit codes

Batch-level failures:

- Unsupported provider.
- Invalid JSON/YAML file.
- Missing required fields.
- Invalid provider config schema.
- Failed external provider credential validation.

These stop execution immediately and return exit code `1`.

Per-app failures:

- App does not exist.
- DB write failure for a specific app.
- Unexpected per-app exception.

Default behavior is best effort: continue processing remaining apps and summarize failures. With `--fail-fast`, stop after the first per-app failure. If any app fails, the command exits with code `1`.

## Output

Human-readable output should include:

- Provider.
- Whether tracing was enabled.
- Whether external validation was skipped.
- Per-app status: `created`, `updated`, `enabled`, or `failed`.
- Final summary counts.

For CI logs, keep output deterministic and avoid printing secret values. Templates may include placeholder values, but execution output must not echo credentials.

## Testing strategy

Add unit tests for the new service:

- Lists all providers from existing provider enum/map.
- Generates templates with required/default fields.
- Rejects unsupported providers.
- Validates provider config schema.
- `skip_validate=True` does not call external `api_check()`.
- `validate=True` calls external validation once per batch, not once per app.
- Creates config when none exists.
- Updates config when one exists.
- Enables app tracing by default.
- Does not enable app tracing when `enable=False`.
- Continues after per-app failure by default.
- Stops after first per-app failure with `fail_fast=True`.

Add CLI tests where practical:

- `trace-config providers`
- `trace-config template langfuse`
- `trace-config batch --config-json ...`
- `trace-config batch --file ...`
- `trace-config wizard` manual app ID entry
- `trace-config wizard` workspace/app selection path
- Invalid wizard workspace or app number input

## Security considerations

- Do not print user-provided secret values in execution output.
- Keep encrypted storage behavior consistent with existing trace config APIs.
- Reuse tenant-specific encryption through existing encryption helpers.
- Treat `--skip-validate` as an explicit operator choice for CI/offline environments.

## Implementation notes

- YAML file support should use the project's existing `pyyaml` dependency.
- Provider templates should be derived dynamically, but formatting can stay simple for the first iteration.
- Wizard workspace/app listings should be deterministic: order workspaces and apps by creation time or name plus ID as a stable tie-breaker.
- Keep commits scoped so unrelated local modifications are not included.
