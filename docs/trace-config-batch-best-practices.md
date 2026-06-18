# Trace Config Batch Best Practices

This guide describes how to configure one tracing provider for multiple Dify apps with the `flask trace-config` command.

Use the command from an API runtime where the Dify Flask app can access the same database and encryption configuration as the target deployment. In Docker-based environments, run it from the API container or an equivalent one-off API job. Keep provider credentials in CI secrets or an operator secret store, not in committed files.

Supported providers can be discovered at runtime:

```bash
flask trace-config providers
```

## Integration With CI

### User Journey

An application team wants every deployment environment to enable the same tracing provider for a known set of app IDs. The team needs a deterministic flow that can be reviewed, repeated, and failed by CI when anything goes wrong.

1. Discover the provider schema in the target version.

   Generate the template from the running code instead of copying an old example from documentation:

   ```bash
   flask trace-config template phoenix --format yaml
   ```

   For pipeline generators or validation jobs that support every provider, export all templates:

   ```bash
   flask trace-config template --all --format json
   ```

2. Create a CI input file from the template.

   Start with the generated template, replace `"<required>"` placeholders with secret-backed values, and replace `"<app-id>"` with explicit app IDs:

   ```yaml
   provider: phoenix
   app_ids:
     - 5dfabab3-1911-47bd-91fb-512a412a436a
     - 11111111-1111-1111-1111-111111111111
   tracing_config:
     api_key: ${PHOENIX_API_KEY}
     project: production
     endpoint: https://app.phoenix.arize.com
   enable: true
   ```

   Do not commit rendered files that contain real credentials. Prefer generating the final JSON/YAML file inside the CI job from secrets.

3. Run the batch command.

   File input is the safest CI pattern because it keeps the command line stable and avoids shell quoting issues:

   ```bash
   flask trace-config batch --file trace-config.yaml
   ```

   Inline JSON is useful for short one-off jobs:

   ```bash
   flask trace-config batch \
     --provider phoenix \
     --app-ids 5dfabab3-1911-47bd-91fb-512a412a436a,11111111-1111-1111-1111-111111111111 \
     --config-json '{"api_key":"px-xxx","project":"production","endpoint":"https://app.phoenix.arize.com"}'
   ```

4. Let CI enforce the result.

   The command exits with `0` only when every app succeeds. It exits with `1` for invalid input, provider validation failure, or any per-app failure. The output prints the provider, whether tracing was enabled, whether external validation was skipped, each app result, and final counts.

5. Decide how strict the pipeline should be.

   The default behavior is best effort across app IDs: one missing app is reported, remaining apps continue, and the final exit code is still `1`. Add `--fail-fast` when partial progress is less useful than stopping immediately after the first per-app failure.

   Keep external validation enabled for normal deployment pipelines. Use `--skip-validate` only when the provider API is intentionally unreachable from CI, such as offline staging or a network-restricted maintenance job.

### CI Parameters

`flask trace-config template [provider]`

| Parameter | Required | Description |
| --- | --- | --- |
| `provider` | Required unless `--all` is used | Provider name, for example `phoenix`. |
| `--all` | No | Print templates for every supported provider. |
| `--format json\|yaml` | No | Output format. Defaults to `json`. |

Template fields:

| Field | Required | Description |
| --- | --- | --- |
| `provider` | Yes | Provider name normalized by Dify. |
| `app_ids` | Yes | Explicit app IDs to configure. Workspace or name pattern selection is not supported in `batch`. |
| `tracing_config` | Yes | Provider-specific credential/config object. Required fields are shown as `"<required>"`; optional fields include provider defaults. |
| `enable` | No | Whether to enable tracing after credentials are written. Defaults to `true` when omitted from file input. |

`flask trace-config batch`

| Parameter | Required | Description |
| --- | --- | --- |
| `--file <path>` | Required for file mode | JSON or YAML file containing `provider`, `app_ids`, `tracing_config`, and optional `enable`. Do not combine with inline input parameters. |
| `--provider <name>` | Required for inline mode | Provider name. Must be combined with `--app-ids` and `--config-json`. |
| `--app-ids <ids>` | Required for inline mode | Comma-separated app IDs. Empty entries are ignored after trimming. |
| `--config-json <json>` | Required for inline mode | Provider `tracing_config` as a JSON object. |
| `--no-enable` | No | Upsert credentials/config only. Overrides file `enable` and does not enable app tracing. |
| `--skip-validate` | No | Skip the external provider credential check. Schema validation still runs. |
| `--fail-fast` | No | Stop after the first per-app failure instead of continuing through the remaining app IDs. |

## Guided Operations Runbook

### User Journey

An operations engineer needs to configure tracing for several existing apps during a maintenance window. They may not know every app ID up front, so the command should guide selection while still showing a final confirmation before writing.

1. Start the wizard from the API runtime.

   ```bash
   flask trace-config wizard
   ```

2. Choose the tracing provider.

   The wizard prints the supported provider list and prompts for one provider name. The provider must match one of the listed values.

3. Choose how to select apps.

   Use `Select from workspace` when the operator wants to browse the available workspaces and apps. The wizard lists workspaces by number, name, and ID, then lists apps in the selected workspace by number, name, mode, and ID.

   Use `Enter app IDs manually` when the change ticket already contains app IDs. Enter them as a comma-separated list.

4. Enter provider-specific settings.

   The wizard prompts for fields from the same provider template used by CI. Required secret-like fields are hidden while typing. Optional fields with defaults can usually be accepted with Enter.

5. Choose whether to enable tracing and validate credentials.

   The default is to enable tracing after credentials are written. The default is also to validate credentials through the provider API before writing any app config.

6. Review the summary and confirm.

   The wizard prints provider, app IDs, enable behavior, and validation behavior. It writes only after the final `Proceed?` confirmation.

7. Check the per-app result.

   Each selected app is marked as `created`, `updated`, `created, enabled`, `updated, enabled`, or `failed`. If any app fails, the command exits with `1` after printing the summary.

### Wizard Parameters And Prompts

`flask trace-config wizard` does not take command-line options. It collects these values interactively:

| Prompt | Description |
| --- | --- |
| `Provider` | One provider from `flask trace-config providers`. |
| `Mode` | `1` selects apps from a workspace; `2` accepts manually entered app IDs. |
| `Workspace` | Numeric selection from the workspace list. Used only in workspace selection mode. |
| `Apps (comma-separated numbers)` | One or more numeric app selections from the selected workspace. Used only in workspace selection mode. |
| `App IDs (comma-separated)` | Explicit app IDs. Used only in manual mode. |
| Provider fields | Fields from `flask trace-config template <provider>`. Required fields must be entered; optional fields show defaults. |
| `Enable tracing after credentials are written?` | When yes, writes credentials and selects/enables the provider for each app. |
| `Validate credentials with provider API?` | When yes, performs external provider validation once before any app write. |
| `Proceed?` | Final confirmation. Answering no aborts without writing. |

Operational notes:

- The wizard's workspace and app picker is only a selection helper. The write path still uses explicit app IDs.
- The wizard always continues through per-app failures and reports all results; it does not expose a fail-fast prompt.
- The command output does not print credential values.
- Re-running the same provider config is idempotent: existing `TraceAppConfig` rows are updated and missing rows are created.
