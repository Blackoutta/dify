# Phoenix Custom Trace Session ID Design

## Purpose

Allow API callers to set the Phoenix/OpenInference `session.id` used for workflow and chatflow tracing without changing Dify conversation, workflow run, or OpenTelemetry trace identity semantics.

This is scoped to the Phoenix/Arize Phoenix tracing provider only. Other tracing providers keep their current behavior.

## API Contract

Add an optional JSON request field named `trace_session_id` to workflow and chatflow run APIs.

Example workflow request:

```json
{
  "inputs": {},
  "response_mode": "streaming",
  "trace_session_id": "external-session-123"
}
```

Example chatflow request:

```json
{
  "inputs": {},
  "query": "hello",
  "response_mode": "streaming",
  "trace_session_id": "external-session-123"
}
```

The field is an observability hint. It must not be added to workflow inputs, model inputs, node inputs, or persisted conversation state as a business variable.

Validation:

- Accept only non-empty strings after trimming whitespace.
- Reject values longer than 512 characters.
- If omitted, blank, or null, preserve the existing Phoenix session resolution behavior.

## Session Resolution

Phoenix workflow tracing currently resolves `session.id` as:

```python
conversation_id or parent_workflow_run_id or workflow_run_id or ""
```

After this change, Phoenix workflow tracing resolves it as:

```python
trace_session_id or conversation_id or parent_workflow_run_id or workflow_run_id or ""
```

For message/chat spans emitted by the Phoenix provider, use:

```python
trace_session_id or conversation_id
```

The custom value affects only `SpanAttributes.SESSION_ID`. It does not affect:

- OTel `trace_id`
- OTel `span_id`
- `conversation_id`
- `workflow_run_id`
- parent span attachment
- Redis parent carrier bridge behavior

## Data Flow

Controllers parse `trace_session_id` from JSON request bodies for:

- Service API workflow run
- Service API chat messages
- Web workflow run
- Web chat messages
- Console installed-app workflow run
- Console installed-app chat messages
- Console app chat/debug workflow paths where the same generator arguments are used

This is backend support only. Existing Web and Console frontends do not need to add a visible UI or start sending the field in this phase. If those clients send `trace_session_id` later, the backend will already pass it through consistently.

Generators copy the validated value into `AppGenerateEntity.extras` under `trace_session_id`.

Workflow and advanced-chat pipeline completion paths pass this value into `OpsTraceManager` together with the existing `parent_trace_context`.

`OpsTraceManager.workflow_trace()` writes the value into workflow trace metadata as `trace_session_id`.

`ArizePhoenixDataTrace` reads `trace_session_id` from metadata and uses it when setting `SpanAttributes.SESSION_ID`.

## Nested Workflow Behavior

When a top-level workflow/chatflow run has `trace_session_id`, nested workflow-as-tool runs inherit the same value through the existing private extras propagation path.

This keeps all spans from the logical invocation in the same Phoenix session while preserving the existing Redis carrier bridge for actual parent-child span attachment.

If an internal nested workflow explicitly receives a different `trace_session_id`, the explicit child value wins. This is expected only for internal/private calls and should not be exposed as a normal user workflow input.

## Documentation

Update API documentation for workflow run and chat message endpoints to describe `trace_session_id` as an optional Phoenix tracing session override.

The documentation should state that the field affects Phoenix/OpenInference `session.id` only and does not resume or create a Dify conversation.

Do not update frontend UI or frontend request builders in this phase.

## Testing

Add focused tests for:

- Phoenix session resolver preferring `trace_session_id` over conversation, parent workflow, and workflow run IDs.
- Workflow root, workflow, and node spans receiving the custom session ID.
- Nested workflow traces using inherited custom session ID while still resolving parent carrier from `parent_trace_context`.
- Chat/message Phoenix spans using custom session ID.
- Invalid API values being rejected or ignored according to the validation contract.
- Absence of `trace_session_id` preserving existing session resolution behavior.
