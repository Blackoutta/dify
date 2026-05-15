# Phoenix Parent Trace Fallback

## Context

Nested workflow traces carry a `parent_trace_context` so a child workflow span can be attached under the parent workflow's tool node span in Phoenix/Arize. That parent node carrier is published to Redis only when the parent workflow is processed by Dify's app-level Phoenix/Arize tracing provider.

In the observed configuration, the top-level parent workflow does not enable app-level tracing, while nested child workflows do. The child traces therefore contain a parent node reference whose carrier can never be published. The current provider treats this as temporarily pending and retries the ops trace task every 5 seconds until the retry budget is exhausted.

## Goal

Avoid retrying Phoenix/Arize ops trace tasks when the referenced parent workflow cannot publish a parent node carrier because its app-level tracing is disabled or not using Phoenix/Arize.

## Design

When a workflow trace has `parent_trace_context`, the Phoenix provider should resolve the parent node carrier as it does today. If the carrier exists, the child workflow remains attached under the parent tool node span.

If the carrier is missing, the provider should inspect the parent workflow run. When the parent workflow's app is not configured for an enabled Phoenix/Arize app-level tracing provider, the provider should not raise `PendingTraceParentContextError`. Instead, it should create or reuse a synthetic root span keyed by `parent_workflow_run_id`, then attach the child workflow span beneath that root. This preserves grouping by the parent run without inventing a precise parent tool span that was never exported.

If the parent workflow's app is configured for an enabled Phoenix/Arize app-level tracing provider, the missing carrier remains retryable. In that case the parent trace may simply not have been processed yet, so `PendingTraceParentContextError` should continue to trigger the existing Celery retry path.

If the parent workflow run cannot be found, keep the existing retry behavior. That case can still be caused by processing order or transaction visibility.

## Components

- `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
  - Add a helper that determines whether a parent workflow run can publish Phoenix/Arize parent span context.
  - Use the helper when resolving a missing parent carrier.
  - Fall back to `ensure_root_span(parent_workflow_run_id, ...)` only for non-publishable parents.

- `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
  - Add coverage for fallback when the parent app has no app-level tracing.
  - Add coverage that missing carriers remain retryable when the parent app does use Phoenix/Arize tracing.

## Error Handling

The fallback should be narrow. Invalid Redis carrier contents remain hard errors. Missing Redis carrier remains retryable unless the parent workflow's app-level tracing configuration proves the carrier cannot be produced.

## Testing

Run the focused Phoenix trace unit tests and the ops trace task retry tests.
