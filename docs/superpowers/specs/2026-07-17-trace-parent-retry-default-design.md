# Trace Parent Retry Default Design

## Problem

Nested workflow trace tasks can run before the parent workflow finishes and publishes its provider context. The current defaults retry 60 times at five-second intervals, covering only 300 seconds, while `WORKFLOW_MAX_EXECUTION_TIME` defaults to 1,200 seconds.

This affects both legacy Phoenix and unified providers because they share `process_trace_tasks`.

## Design

Change the default retry limit from 60 to 240 while retaining the five-second delay. The default retry window will therefore match the default maximum workflow execution time:

```text
240 retries * 5 seconds = 1,200 seconds
```

Document the recommended relationship beside the configuration:

```text
max_retries >= ceil(WORKFLOW_MAX_EXECUTION_TIME / retry_delay_seconds)
```

Use explicit defaults rather than calculating the value at runtime. Deployments can still tune both settings independently.

Update API and Docker environment examples to use 240. Existing deployments that explicitly configure 60 remain unchanged.

## Test

Replace the Redis-TTL-only retry-window assertion with an assertion that the configured task retry window covers `WORKFLOW_MAX_EXECUTION_TIME`.

## Scope

Do not change retry scheduling, Celery persistence, Redis context TTL, provider adapters, or legacy/unified routing.
