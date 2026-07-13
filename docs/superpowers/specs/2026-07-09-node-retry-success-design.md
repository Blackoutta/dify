# Node Retry Success Trace Design

## Problem

`workflow_runs.exceptions_count` tells users how many node failures happened during a workflow run, but the
historical node execution API only exposes each node's final execution row. When a node fails, retries, and then
succeeds, the persisted node row ends as `status: "succeeded"` and loses the retry failure details. Users cannot tell
which node caused the workflow-level exception count.

## Goal

For historical workflow traces, show a node that retried and eventually succeeded as "retry succeeded" in the UI while
keeping its real API status as `succeeded`. Preserve prior retry errors in the node's `process_data` so the existing
node execution API can expose the details without a schema migration.

## Scope

This applies to every run path that persists node executions through `WorkflowPersistenceLayer`, including workflow
apps, advanced-chat/chatflow apps, and RAG pipeline workflow runs. The app log endpoint currently covers
`ADVANCED_CHAT` and `WORKFLOW`; RAG pipeline uses a separate node-execution endpoint but returns the same node execution
field model and renders through the same trace panel.

This is node-type agnostic. HTTP, LLM, Tool, and any future node type that supports retry should benefit as long as it
emits the common `NodeRunRetryEvent`. The implementation must not add HTTP-specific or LLM-specific branches.

Non-workflow app types that do not create workflow runs or workflow node executions are outside this change.

## Non-Goals

- Do not add a new database column.
- Do not add a new `WorkflowNodeExecutionStatus` value.
- Do not change `status` in `/workflow-runs/<run_id>/node-executions` from `succeeded` to a new value.
- Do not redesign retry history UI; reuse the current trace detail surfaces.

## Backend Design

The persistence layer already receives `NodeRunRetryEvent` before a later `NodeRunSucceededEvent` for the same node
execution. It should use that ordering to preserve retry history on the existing `WorkflowNodeExecution.process_data`.

On `NodeRunRetryEvent`, append a retry entry to `process_data.retry_errors`:

```json
{
  "retry_index": 1,
  "error": "Reached maximum retries for URL http://localhost:18080/test"
}
```

If `process_data` already contains node-specific keys, keep them and add `retry_errors`. If it is empty, create a new
mapping containing only `retry_errors`.

On the later successful node result, merge any existing `retry_errors` from the cached persisted execution into the
final node result `process_data` before saving. This prevents the final successful `process_data` from overwriting retry
history.

The historical API response stays backward compatible:

```json
{
  "status": "succeeded",
  "process_data": {
    "request": "GET /test HTTP/1.1\r\nHost: localhost:18080\r\n\r\n",
    "retry_errors": [
      {
        "retry_index": 1,
        "error": "Reached maximum retries for URL http://localhost:18080/test"
      }
    ]
  }
}
```

## Frontend Design

Add a derived UI state in the workflow trace node panel:

```ts
const isRetrySucceeded = nodeInfo.status === 'succeeded'
  && Array.isArray(nodeInfo.process_data?.retry_errors)
  && nodeInfo.process_data.retry_errors.length > 0
```

When `isRetrySucceeded` is true:

- Render a yellow warning icon instead of the green success check.
- Show a short status block labeled with the existing retry wording, such as "Retry successful".
- Show a warning banner immediately above the Process Data editor, using the same warning styling as the existing
  node exception banners. The banner text should say that an exception retry was detected and details are available in
  this Process Data section.
- Leave the node's underlying `status` unchanged.
- Keep the normal process data panel visible so users can inspect `retry_errors`.

Frontend strings should use `web/i18n/en-US/` and the existing workflow retry namespace where possible. The banner
message should be added as a new i18n key instead of hardcoded in the component.

## Tests

Backend:

- Add or update a focused persistence-layer unit test that emits retry then success for one node.
- Assert the final node execution has `status == SUCCEEDED`.
- Assert `process_data.retry_errors` contains the retry index and error.
- Assert final node-specific process data remains present after the merge.

Frontend:

- Add or update a focused `NodePanel` test for `status: "succeeded"` with `process_data.retry_errors`.
- Assert the retry-success label is shown.
- Assert the Process Data warning banner is shown with the i18n message.
- Assert the success-only green check path is not used for this derived state.

## Compatibility

Existing API consumers that read `status` continue to see `succeeded`. Consumers that render `process_data` receive one
additional optional key, `retry_errors`, only for nodes that retried. Existing traces without retry history are
unchanged.
