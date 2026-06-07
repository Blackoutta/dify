# Async Workflow Log Publishing via ActiveMQ

## Context

High-volume customer deployments can exhaust database connections and overload the database primarily because workflow node execution logs are written synchronously and frequently. Workflow run records are comparatively low volume and are still used as an anchor by existing app-log, response, and tracing paths. The current node execution repository persists each save through SQLAlchemy and commits on the workflow execution path; complex workflows amplify this because every node can produce one or more writes.

The target is to reduce database pressure for production workflow invocations while preserving the existing console debugging experience.

## Goals

- Asynchronously publish workflow node execution log writes for non-debugging workflow invocations.
- Keep workflow run records synchronous for all invocation types.
- Keep console debugging and single-step debugging node execution writes synchronous.
- Keep existing workflow tracing complete when async workflow log publishing is enabled.
- Use ActiveMQ in this version.
- Keep the Dify API side decoupled from ActiveMQ-specific details through a publisher abstraction.
- Reuse ActiveMQ publisher connections safely at API-process scope instead of creating one connection per workflow run.
- Retry transient ActiveMQ connect/send failures by resetting the STOMP connection and reconnecting before dropping an event.
- Use fail-open behavior: workflow execution must continue even if log publishing fails.
- Do not implement the consumer in this repository.
- Preserve backward compatibility by keeping async publishing disabled by default.

## Non-Goals

- Implementing the Go consumer service.
- Supporting Kafka or other queue providers in this version.
- Supporting ActiveMQ topic broadcasting in this version.
- Adding a local durable buffer inside Dify API.
- Adding an ActiveMQ connection pool in the first reliability fix; use one reusable publisher connection per API worker process first.
- Changing database schema.
- Making console debugging asynchronous.
- Guaranteeing exactly-once delivery.
- Requiring the future consumer to finish node execution database writes before tracing can run.

## Recommended Approach

Use **ActiveMQ + STOMP + Queue**.

Reasons:

- STOMP is simple for Python producers.
- ActiveMQ Classic and ActiveMQ Artemis commonly support STOMP.
- A queue supports multiple Go consumer pods through competing consumers.
- The log use case does not need JMS/OpenWire-specific features.
- Use `stomp.py` as an optional/lazy dependency so default installations do not fail when async publishing is disabled.

The ActiveMQ broker must expose a STOMP connector, commonly on port `61613`.

## Architecture

```text
Workflow Runtime
   |
   v
WorkflowExecutionRepository / WorkflowNodeExecutionRepository
   |
   v
Workflow Log Write Strategy
   |
   |-- WorkflowRun: always synchronous SQLAlchemy write
   |
   |-- WorkflowNodeExecutionModel
         |-- synchronous SQLAlchemy write for DEBUGGING, single-step, or async disabled
         |-- WorkflowLogPublisher abstraction for non-debugging async mode
               |
               |-- ActiveMQ STOMP publisher
```

Affected write paths:

- `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`
  - `WorkflowNodeExecutionModel`

`api/core/repositories/sqlalchemy_workflow_execution_repository.py` remains synchronous and is not part of the async publishing path.

The node execution repository should not depend directly on ActiveMQ client APIs. It should only decide whether to use synchronous DB persistence or publish a node execution log event through the abstraction. The workflow execution repository continues to persist `WorkflowRun` synchronously.

## Write Routing Rules

1. Always save `WorkflowRun` synchronously to DB.
2. If async workflow node log publishing is disabled, keep node execution synchronous DB writes.
3. If the workflow invocation is console debugging or single-step debugging, keep node execution synchronous DB writes.
4. Otherwise, publish workflow node execution events to ActiveMQ.
5. If publishing fails, log the failure and drop the node execution log event. Do not raise the error to the workflow execution path and do not fall back to synchronous node execution DB writes.

This means Service API, WebApp, and Installed App production invocations can use asynchronous node execution log publishing, while workflow runs and console debugging remain immediately queryable from the database.

### Debugging Detection

Workflow run and node execution repositories use different trigger enums today:

- `SQLAlchemyWorkflowExecutionRepository` receives `WorkflowRunTriggeredFrom.DEBUGGING` or `WorkflowRunTriggeredFrom.APP_RUN`.
- `SQLAlchemyWorkflowNodeExecutionRepository` receives `WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN` for full workflow runs and `WorkflowNodeExecutionTriggeredFrom.SINGLE_STEP` for single-step runs.

A full console debugging run still uses `WorkflowNodeExecutionTriggeredFrom.WORKFLOW_RUN` for node executions, so node execution async routing must not rely only on the node repository's `triggered_from` value.

Implementation should introduce an explicit write-mode signal, for example:

```python
class WorkflowLogWriteMode(StrEnum):
    SYNC = "sync"
    ASYNC = "async"
```

Repository factories or app generators should derive this node execution write mode from `invoke_from` / workflow run trigger once, then pass it to the node execution repository. The workflow execution repository does not need this write mode because workflow runs remain synchronous. To preserve backward compatibility across existing node repository call sites, any new constructor parameter for write mode must default to `WorkflowLogWriteMode.SYNC`.

- `InvokeFrom.DEBUGGER` -> `WorkflowLogWriteMode.SYNC`
- single-step debugging -> `WorkflowLogWriteMode.SYNC`
- Service API / WebApp / Installed App with async enabled -> `WorkflowLogWriteMode.ASYNC` for node executions only
- any invocation with async disabled -> `WorkflowLogWriteMode.SYNC`

This keeps debugging behavior consistent while leaving workflow run writes synchronous for every mode.

## Repository Runtime Semantics

Async node publishing changes node execution persistence but must not change repository semantics used by the workflow runtime.

### Read-after-write Cache

`WorkflowCycleManager` saves a workflow execution at run start and later reads it from the same repository instance when the run succeeds, partially succeeds, or fails. Node executions are also saved at node start and then read by `get_by_node_execution_id()` when the node succeeds or fails.

Therefore, async `save()` must still update repository in-memory cache exactly as the synchronous path does:

- `SQLAlchemyWorkflowExecutionRepository.save()` remains synchronous and keeps its existing cache behavior.
- `SQLAlchemyWorkflowNodeExecutionRepository.save()` must cache the `WorkflowNodeExecutionModel` by `node_execution_id`, even if it publishes instead of committing.
- `get()` and `get_by_node_execution_id()` must continue checking cache before querying DB.

Within one workflow execution, repository read-after-write behavior must remain identical to the current synchronous implementation.

### Log Query Consistency

Workflow runs, console debugging node executions, and single-step debugging node executions remain synchronously persisted and immediately queryable from existing log APIs.

For non-debugging invocations using async workflow node log publishing, existing APIs that create a new repository and query node executions from DB directly are eventually consistent. Before the external consumer writes node events to DB, these APIs may temporarily return:

- an empty or incomplete node execution list,
- node executions without the latest terminal status.

The workflow run record itself remains synchronously persisted and should remain available through existing workflow run query APIs.

This eventual consistency applies only to log query/read APIs. It must not affect workflow execution correctness or tracing completeness.

### Running Node Lookup on Workflow Failure

`WorkflowCycleManager.handle_workflow_run_failed()` calls `get_running_executions()` to find nodes that started but did not emit terminal events. In async mode, those running node rows may not exist in DB yet.

`get_running_executions()` must therefore include cached running nodes in addition to DB rows:

1. Read matching running node executions from `_node_execution_cache` for the workflow run.
2. If the repository is in synchronous mode, also query DB as it does today and merge/deduplicate results by `node_execution_id` or `id`.
3. If the repository is in async mode, cache data is the authoritative source for same-run failure completion; DB querying is optional and must not be required for correctness.
4. Return cached running nodes so the failure handler can publish terminal `FAILED` updates for them.

This keeps failure logs complete without reintroducing synchronous node-start DB writes.

## Tracing Requirements

Workflow tracing is a hard requirement and must continue to work when async workflow node log publishing is enabled. Tracing may rely on synchronously persisted `workflow_runs`, but must not depend on the future consumer having already written `workflow_node_executions` rows to DB.

Current tracing integrations can create fresh repositories and query DB by `workflow_run_id` to build node spans. Arize/Phoenix is especially important because customer deployments rely on it, and its provider currently has a DB-row-like `_get_workflow_nodes()` path. That is unsafe in async mode because the trace task can run before log events are consumed. The implementation must provide a runtime snapshot path:

1. At workflow completion, pass the already-mutated final `WorkflowExecution` object directly to the trace task. Do not rely on reloading the final workflow state from repository cache before the final save updates it.
2. Collect all same-run node execution snapshots after terminal node updates have been saved to repository cache.
3. Pass JSON-compatible snapshots into the workflow trace task or into `WorkflowTraceInfo`.
4. Trace preprocessing may continue to build workflow-level trace data from the synchronous DB `WorkflowRun` row or the already-mutated final workflow object.
5. All workflow trace providers, including Langfuse, Langsmith, Weave, Opik, Aliyun, and Arize/Phoenix, must prefer the provided node execution snapshots when present.
6. DB node lookups remain as fallback for synchronous/default paths, historical trace tasks, and compatibility.

Trace snapshots must be JSON-safe because `TraceTask` data is converted with `model_dump_json()`, saved to OPS storage, and later read by Celery. Snapshots must not contain SQLAlchemy models or raw domain objects. Datetimes, enums, metadata keys, and nested values must be serialized into JSON-compatible primitives before storage, and the Celery worker must be able to dispatch traces without querying DB to reconstruct these snapshots.

Required behavior:

- Async log mode must produce complete workflow traces and node spans from runtime snapshots.
- Arize/Phoenix workflow tracing must continue to produce node spans without waiting for DB log persistence.
- Trace generation must not wait for ActiveMQ consumer node database writes.
- Trace generation must not force node execution logs back to synchronous DB writes.
- Debugging/default sync paths should keep existing DB fallback behavior.

### Trace Snapshot DTOs

Define explicit JSON-compatible DTO shapes so providers do not guess whether fields are domain-style or DB-row-style.

Workflow trace snapshot is optional because `WorkflowRun` remains synchronous. If passed, it should include at minimum the fields already needed to build `WorkflowTraceInfo` without re-querying the DB row:

```json
{
  "id": "workflow_run_id",
  "tenant_id": "tenant_id",
  "app_id": "app_id",
  "workflow_id": "workflow_id",
  "triggered_from": "app-run",
  "type": "workflow",
  "version": "1",
  "graph": {},
  "inputs": {},
  "outputs": {},
  "status": "succeeded",
  "error": null,
  "elapsed_time": 1.23,
  "total_tokens": 100,
  "total_steps": 3,
  "exceptions_count": 0,
  "created_at": "2026-06-06T00:00:00Z",
  "finished_at": "2026-06-06T00:00:01Z"
}
```

Node trace snapshot should include at minimum:

```json
{
  "id": "node_execution_record_id",
  "workflow_run_id": "workflow_run_id",
  "node_execution_id": "node_execution_id",
  "node_id": "node_id",
  "node_type": "llm",
  "title": "LLM",
  "inputs": {},
  "process_data": {},
  "outputs": {},
  "status": "succeeded",
  "error": null,
  "elapsed_time": 1.23,
  "metadata": {},
  "created_at": "2026-06-06T00:00:00Z",
  "finished_at": "2026-06-06T00:00:01Z"
}
```

Provider adapters that need DB-row-like names, such as Arize/Phoenix's current `execution_metadata` and `workflow_run_id` access pattern, should adapt from this DTO instead of querying DB in async mode.

## Publisher Abstraction

Introduce a small abstraction dedicated to workflow log publication. The initial implementation only needs ActiveMQ, but the interface should allow a future Kafka publisher without changing repository logic.

Suggested package layout:

```text
api/core/workflow/log_publisher/
  __init__.py
  entities.py
  publisher.py
  factory.py
  activemq_publisher.py
  noop_publisher.py
```

Suggested interface:

```python
class WorkflowLogPublisher(Protocol):
    def publish_node_execution(self, payload: WorkflowNodeExecutionLogPayload) -> None:
        raise NotImplementedError
```

`noop_publisher.py` is useful for disabled or unsupported configurations and for tests.

## Configuration

Add configuration with safe defaults:

```env
WORKFLOW_LOG_ASYNC_ENABLED=false
WORKFLOW_LOG_QUEUE_PROVIDER=activemq
WORKFLOW_LOG_ACTIVEMQ_HOST=localhost
WORKFLOW_LOG_ACTIVEMQ_PORT=61613
WORKFLOW_LOG_ACTIVEMQ_USERNAME=
WORKFLOW_LOG_ACTIVEMQ_PASSWORD=
WORKFLOW_LOG_ACTIVEMQ_DESTINATION=/queue/dify.workflow.logs
WORKFLOW_LOG_PUBLISH_TIMEOUT=0.2
WORKFLOW_LOG_PUBLISH_MAX_RETRIES=1
```

Notes:

- `WORKFLOW_LOG_ASYNC_ENABLED=false` preserves existing behavior by default.
- This version should accept only `activemq` as the queue provider when async publishing is enabled.
- ActiveMQ client imports should be lazy or optional so default installations are not broken when async publishing is disabled.
- If async publishing is enabled and the STOMP client dependency is unavailable, startup or first publisher creation should fail clearly instead of silently pretending logs are being published.
- `WORKFLOW_LOG_PUBLISH_MAX_RETRIES=1` means one retry after the initial failed connect/send attempt. The default preserves fail-open behavior while recovering common stale connection and broker-side idle close cases.

## Message Format

Use a stable envelope so the future Go consumer is not coupled to Python ORM internals. Producers should serialize naive datetimes as UTC ISO-8601 strings. Consumers should treat missing timezone information as UTC for backward compatibility.

Node execution event:

```json
{
  "event_id": "uuid",
  "event_type": "workflow_node_execution.upsert",
  "schema_version": 1,
  "created_at": "2026-06-06T00:00:00Z",
  "payload": {
    "id": "node_execution_record_id",
    "tenant_id": "tenant_id",
    "app_id": "app_id",
    "workflow_id": "workflow_id",
    "workflow_run_id": "workflow_run_id",
    "node_execution_id": "node_execution_id",
    "node_id": "node_id",
    "node_type": "llm",
    "title": "LLM",
    "triggered_from": "workflow-run",
    "index": 1,
    "predecessor_node_id": "start",
    "inputs": {},
    "process_data": {},
    "outputs": {},
    "status": "succeeded",
    "error": null,
    "elapsed_time": 1.23,
    "execution_metadata": {},
    "created_by_role": "end_user",
    "created_by": "user_id",
    "created_at": "2026-06-06T00:00:00Z",
    "finished_at": "2026-06-06T00:00:01Z"
  }
}
```

The exact payload fields should be derived from the existing SQLAlchemy model mapping so the consumer has enough data to perform an idempotent upsert. JSON-like fields in the message payload should be JSON objects or arrays, not pre-serialized DB text. The consumer is responsible for serializing them into the current database column representation when writing to Dify tables.

## ActiveMQ Headers

Publish messages to a queue destination, for example:

```text
/queue/dify.workflow.logs
```

Set `JMSXGroupID` to the workflow run id when available:

```text
JMSXGroupID: <workflow_run_id>
```

This helps multiple Go consumer pods process events from the same workflow on the same consumer, reducing reordering risk. It does not replace consumer-side idempotency and ordering protections.

Suggested additional headers:

```text
event_type: workflow_node_execution.upsert
schema_version: 1
content_type: application/json
```

## Multiple Consumer Pods

The future consumer service should use queue competing consumers:

```text
Dify API -> ActiveMQ Queue -> Go consumer pod A
                           -> Go consumer pod B
                           -> Go consumer pod C
```

Each message is consumed by one pod. The consumer must handle at-least-once delivery:

- Use idempotent database upserts.
- Acknowledge messages only after the batch database write succeeds.
- Handle duplicate delivery.
- Avoid stale updates overwriting newer states with explicit ordering rules:
  - terminal statuses must not be overwritten by `running` events.
  - events with `finished_at = null` may fill missing fields but must not downgrade a terminal status.
  - terminal vs terminal conflicts should prefer the event with the later `finished_at`.
  - terminal events with the same `finished_at` must use a deterministic status precedence instead of broker delivery order. Suggested precedence, from lower to higher: `succeeded`, `partial-succeeded`, `stopped`, `failed` / `exception`.
- Send poison messages to a dead-letter path according to the consumer project's policy.

## Publisher Connection Lifecycle

The first reliability fix should use a **process-level singleton ActiveMQ publisher with one reusable STOMP connection per API worker process**. Do not add a connection pool in this version unless later load tests prove the single per-worker connection is a bottleneck.

Rationale:

- The observed failure is consistent with per-workflow-run publisher/connection lifecycle leakage or stale connection handling, not proven send throughput saturation.
- Dify API deployments commonly run multiple worker processes; each process gets its own singleton publisher and therefore its own STOMP connection.
- A single connection guarded by a lock is simpler and safer than a pool for `stomp.py`, where concurrent send behavior across shared connections is not the design target.
- A pool can be added later behind the same `WorkflowLogPublisher` abstraction if metrics show lock wait or publish latency is too high.

Expected behavior:

- `create_workflow_log_publisher(config)` returns a process-level cached publisher for the same ActiveMQ configuration instead of allocating a new publisher for every workflow run.
- Connection reuse is per API process. Multi-process workers maintain independent publishers and independent STOMP connections.
- Publish, reconnect, and close operations must be thread-safe because app worker threads may share the singleton publisher.
- Lazily create the STOMP connection on first publish.
- Before reusing a connection, check whether the client exposes an `is_connected()` method; if it exists and returns false, reset and reconnect.
- Reuse the connection for subsequent publishes in the API process.
- Apply a short publish timeout from `WORKFLOW_LOG_PUBLISH_TIMEOUT` to connection creation and send operations.
- On connect or send failure, close/reset the connection and retry up to `WORKFLOW_LOG_PUBLISH_MAX_RETRIES` times.
- If all attempts fail, raise to the repository so the repository can log context and drop the event according to fail-open semantics.
- Register a process-exit cleanup hook or equivalent application shutdown hook that disconnects the singleton publisher.
- Do not block workflow execution on long broker reconnect loops; retries must be bounded and short.

### Retry Semantics

A publish attempt includes both `_ensure_connection()` and `connection.send(...)`.

For each event:

1. Try to ensure a connected STOMP connection and send the event.
2. If connect or send raises, reset/disconnect the current connection.
3. Retry while attempts remain.
4. After the final failed attempt, raise the exception to the repository.
5. The repository catches the exception, logs `workflow_run_id` and `node_execution_id`, increments failure/drop metrics when available, and returns without synchronous DB fallback.

With the default `WORKFLOW_LOG_PUBLISH_MAX_RETRIES=1`, an event gets the initial attempt plus one reconnect attempt. This should recover common `stomp.exception.NotConnectedException` cases caused by stale connections or broker-side idle close, while preserving workflow latency bounds.

## Security Notes

Workflow inputs, outputs, process data, and metadata can contain sensitive customer data. ActiveMQ deployment should follow minimum security requirements:

- Use broker credentials; do not allow anonymous publishing in production.
- Prefer TLS/STOMP-over-SSL when traffic crosses hosts or networks.
- Keep the broker on a private network and do not expose it publicly.
- Use a least-privilege broker user scoped to the workflow log queue where possible.
- Treat queue retention, dead-letter queues, and broker backups as sensitive data stores.
- Treat trace snapshots stored for Celery dispatch in OPS storage as sensitive workflow data with the same access control and retention expectations.

## Failure Handling

Publishing failures are fail-open:

- Catch publisher exceptions.
- Log a warning or error with enough context to troubleshoot.
- Increment metrics when available.
- Drop the log event.
- Continue workflow execution normally.
- Do not fall back to synchronous node execution database writes, because fallback can recreate the database overload this feature is meant to avoid.

Recommended metrics:

- `workflow_log_publish_success_total`
- `workflow_log_publish_failed_total`
- `workflow_log_publish_retry_total`
- `workflow_log_publish_dropped_total`
- `workflow_log_publish_latency_seconds`
- `workflow_log_publisher_reconnect_total`
- `workflow_log_publisher_connection_reset_total`

Metrics are not a substitute for fail-open behavior. They should be added as soon as the project has a suitable metrics hook for this path, because they are the primary way to detect silent node-log loss during load tests and production operation.

## Testing Strategy

- Unit-test routing decisions:
  - async disabled uses synchronous DB write.
  - workflow runs always use synchronous DB write.
  - full console debugging node executions use synchronous DB write even though their node trigger is `WORKFLOW_RUN`.
  - single-step node executions use synchronous DB write.
  - non-debugging with async enabled publishes events.
  - publisher exceptions do not propagate.
- Unit-test repository cache semantics:
  - workflow run synchronous save behavior remains unchanged.
  - node execution async save can be read back by `get_by_node_execution_id()` from the same repository instance.
  - async `get_running_executions()` returns cached running nodes for workflow failure completion.
- Unit-test tracing snapshot behavior:
  - async workflow trace preprocessing can build node spans without DB node execution rows.
  - node trace snapshots survive `model_dump_json()` and Celery-side reconstruction without SQLAlchemy/domain objects.
  - all workflow trace providers prefer provided node execution snapshots over DB lookups.
  - Arize/Phoenix produces workflow node spans from snapshots when DB node rows are absent.
  - node spans are still produced when the DB has not yet been updated by the consumer.
- Unit-test payload serialization for node execution events, including node `triggered_from`.
- Unit-test ActiveMQ publisher headers, especially `JMSXGroupID`.
- Unit-test ActiveMQ publisher retry behavior:
  - stale/not-connected send resets the connection and retries on a new connection.
  - connect failure resets and retries before dropping.
  - exhausted retries clear the cached connection and raise to the repository.
  - `close()` disconnects the cached connection and is safe to call repeatedly.
- Unit-test factory singleton behavior:
  - repeated calls with the same ActiveMQ config return the same process-level publisher.
  - changed ActiveMQ config creates a new publisher so tests and reconfiguration do not reuse stale settings.
  - disabled async publishing still returns a no-op publisher.
- Unit-test provider validation: async enabled with unsupported provider should fail configuration or fall back explicitly according to implementation choice.
- Keep existing repository tests passing for default configuration.
- Integration/load-test the original failure mode: run two consecutive production workflow pressure rounds, such as `hey -n 1000 -c 50`, and assert the second round creates ActiveMQ enqueue events and corresponding `workflow_node_executions` after consumer drain.

## Compatibility

This design is backward-compatible because async publishing is disabled by default, workflow runs remain synchronous, and debugging remains synchronous. Existing deployments without ActiveMQ continue using the current SQLAlchemy persistence path.

## Open Implementation Notes

- Prefer lazy importing the STOMP client in the ActiveMQ publisher so installations without the optional dependency are unaffected unless async publishing is enabled.
- Reuse existing model-to-DB mapping methods where possible to avoid duplicating field conversion logic.
- Add an explicit node execution write-mode constructor parameter rather than inferring node debugging from `WorkflowNodeExecutionTriggeredFrom`; default it to `WorkflowLogWriteMode.SYNC` for backward compatibility.
- Keep cache updates shared between sync and async save paths to preserve workflow runtime behavior.
- Add repository or trace-task accessors for same-run node execution snapshots so tracing does not depend on async node DB writes.
- Keep trace snapshot DTOs JSON-compatible and provider-neutral; add provider-specific adapters where existing providers expect DB-row-like fields.
- Keep the publisher interface small and dedicated to workflow logs; do not generalize it into a broad messaging framework in this version.
- Prefer the process-level singleton plus bounded retry before adding a connection pool. Add a pool only if measured publish latency or lock contention shows the singleton is the bottleneck.
