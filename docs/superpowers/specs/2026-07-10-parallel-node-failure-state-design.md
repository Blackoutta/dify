# Parallel Node Failure State Design

## Goal

Distinguish the node that actually failed from unfinished nodes stopped by the same fail-fast workflow run.

## Behavior

- The originating node remains `failed` and keeps its real `inputs`, `process_data`, `outputs`, and `error`.
- Other in-flight nodes stopped by that failure become `stopped`.
- Stopped nodes receive synthetic `outputs` describing the cause:

```json
{
  "failed_node_id": "node-id",
  "failed_node_title": "Node title",
  "error": "Original node error"
}
```

- Existing frontend rendering is unchanged; it already supports the `stopped` status and displays node outputs.
- Nodes that completed before the failure keep their actual terminal status and data.

## Backend Flow

1. Preserve the originating `NodeRunFailedEvent` exactly as today.
2. Carry the originating node ID through the graph-level failure event.
3. During workflow failure persistence, resolve the originating node title from the execution cache.
4. Change other in-flight `running` or `retry` executions to `stopped` and write the structured cause to their outputs.
5. Do not overwrite terminal node executions.

## Compatibility

- Reuse the existing `WorkflowNodeExecutionStatus.STOPPED` value.
- Keep existing fields and API response shapes; only stopped nodes gain structured outputs.
- Preserve current `exception` semantics for fail-branch and default-value error handling.

## Tests

- A real failing node remains `failed` with its original execution data.
- A parallel unfinished node becomes `stopped` with the expected cause outputs.
- A retrying parallel node also becomes `stopped`.
- A previously completed parallel node is not changed.
- Workflow abort behavior remains separate from failure propagation.
