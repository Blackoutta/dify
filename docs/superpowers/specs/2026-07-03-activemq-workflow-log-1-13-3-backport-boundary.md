# ActiveMQ Workflow Node Log Backport Boundary for Dify 1.13.3

## Goal

Backport the workflow node execution log ActiveMQ path to Dify 1.13.3 with the smallest useful scope.

The backport moves production workflow node execution writes out of the Dify API hot path and into the Go consumer, while preserving debugger and internal workflow behavior.

## Final Scope

### Dify Side

- Add a dedicated migration file for:

  ```sql
  alter table workflow_node_executions add column state_version bigint null;
  ```

- Add ActiveMQ producer configuration, default disabled.
- Add a producer-only workflow node execution repository.
- Publish `workflow_node_execution.upsert` events to ActiveMQ when async logging is enabled.
- Include `state_version` in the event payload.
- Generate `state_version` in memory per `workflow_node_executions.id`.
- Protect the in-memory cache and `state_version` increment with a lock.
- Do not publish to ActiveMQ while holding the lock.
- Do not roll back `state_version` after publish failure.

### Async Routing Rule

Only this path goes to ActiveMQ:

```text
triggered_from == WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN
workflow_triggered_from == WorkflowRunTriggeredFrom.APP_RUN
```

Meaning: published app runs triggered by users or service API calls.

These paths stay synchronous:

- console debugger runs
- single-step node runs
- draft workflow previews
- RAG pipeline runs and RAG pipeline debugging
- ops tracing repository usage
- any caller that does not explicitly provide `workflow_triggered_from == APP_RUN`

This is intentionally conservative for the 1.13.3 backport.

### Consumer Side

- Keep `paused` status support.
- Keep `state_version` parsing, schema validation, insert, and conflict handling.
- Keep simplified truncation behavior.
- Write truncated JSON previews directly into:
  - `workflow_node_executions.inputs`
  - `workflow_node_executions.process_data`
  - `workflow_node_executions.outputs`
- Continue compacting and writing `execution_metadata` directly.
- Use `state_version` as the primary conflict-ordering signal.
- Keep status and `finished_at` ordering as fallback compatibility logic.

## Explicit Non-Scope

The 1.13.3 backport will not include workflow node execution offload support.

Consumer side must not:

- write object storage artifacts for node execution fields
- write `upload_files`
- write `workflow_node_execution_offload`
- require offload storage configuration
- run offload integration tests as part of this backport

Dify side must not:

- port unrelated Graphon/package refactors
- port docker env restructuring from later versions
- make ActiveMQ logging default-on
- change debugger or single-step persistence behavior

## Truncation Rule

Use the consumer's simplified truncation behavior, not a strict copy of Dify's `VariableTruncator`.

- Compact JSON first.
- If compact JSON size is within `WORKFLOW_VARIABLE_TRUNCATION_MAX_SIZE`, write it as-is.
- If it exceeds the limit:
  - truncate strings by rune count using `WORKFLOW_VARIABLE_TRUNCATION_STRING_LENGTH`
  - truncate arrays to `WORKFLOW_VARIABLE_TRUNCATION_ARRAY_LENGTH`
  - recursively apply truncation inside objects and arrays
- Write the truncated JSON directly to the original `workflow_node_executions` text columns.

## State Version Rule

The Dify producer maintains a per-repository in-memory map:

```text
workflow_node_executions.id -> latest state_version
```

For each saved node execution snapshot:

1. Lock.
2. Update in-memory read cache.
3. Increment `state_version`.
4. Build the event payload.
5. Unlock.
6. Publish to ActiveMQ.

The lock protects only in-memory state. ActiveMQ I/O must happen outside the lock.

Cross-process ordering is not guaranteed. The backport accepts this because adding DB or Redis counters would reintroduce the hot-path dependency this feature is meant to remove.

## Consumer Conflict Rule

When `state_version` is present on both existing and incoming rows, the incoming row wins only when:

```text
incoming.state_version >= existing.state_version
```

When version data is missing, fall back to conservative status ordering:

- final states are not overwritten by `running`, `paused`, or `retry`
- `running` and `paused` may overwrite each other
- `retry` may replace `running` or `paused`
- final states may replace `retry`
- final-vs-final uses `finished_at`, then final status priority

## Verification Boundary

### Dify

Run focused checks only:

- config defaults for async workflow logging
- factory routing for `WORKFLOW_RUN + APP_RUN`
- synchronous fallback for debugger and single-step
- ActiveMQ repository event payload and locked `state_version` increment
- migration test for nullable `state_version`

### Consumer

Run focused checks only:

- event parser accepts `paused` and `state_version`
- schema check requires `workflow_node_executions.state_version`
- upsert writes `state_version`
- conflict handling prefers newer `state_version`
- simplified truncation writes previews directly to node execution columns
- handler no longer requires offload store or offload writer

## Implementation Order

1. Update consumer first:
   - remove offload storage and metadata write path
   - keep simplified truncation
   - keep `paused` and `state_version`
2. Update Dify 1.13.3:
   - add migration
   - add producer config
   - add ActiveMQ producer repository
   - route only `WORKFLOW_RUN + APP_RUN`
3. Run focused tests in each repository.
4. Commit consumer and Dify changes separately.

