# Async Workflow Log Publishing via ActiveMQ

## Context

High-volume customer deployments can exhaust database connections and overload the database because workflow run logs and node execution logs are written synchronously and frequently. The current repository implementations persist each save through SQLAlchemy and commit on the request/workflow execution path. Complex workflows amplify this because every workflow run and every node execution can produce one or more writes.

The target is to reduce database pressure for production workflow invocations while preserving the existing console debugging experience.

## Goals

- Asynchronously publish workflow run and workflow node execution log writes for non-debugging workflow invocations.
- Keep console debugging and single-step debugging synchronous.
- Keep existing workflow tracing complete when async workflow log publishing is enabled.
- Use ActiveMQ in this version.
- Keep the Dify API side decoupled from ActiveMQ-specific details through a publisher abstraction.
- Use fail-open behavior: workflow execution must continue even if log publishing fails.
- Do not implement the consumer in this repository.
- Preserve backward compatibility by keeping async publishing disabled by default.

## Non-Goals

- Implementing the Go consumer service.
- Supporting Kafka or other queue providers in this version.
- Supporting ActiveMQ topic broadcasting in this version.
- Adding a local durable buffer inside Dify API.
- Changing database schema.
- Making console debugging asynchronous.
- Guaranteeing exactly-once delivery.
- Requiring the future consumer to finish database writes before tracing can run.

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
   |-- Synchronous SQLAlchemy write
   |     - DEBUGGING
   |     - async disabled
   |
   |-- WorkflowLogPublisher abstraction
         |
         |-- ActiveMQ STOMP publisher
```

Affected write paths:

- `api/core/repositories/sqlalchemy_workflow_execution_repository.py`
  - `WorkflowRun`
- `api/core/repositories/sqlalchemy_workflow_node_execution_repository.py`
  - `WorkflowNodeExecutionModel`

The repositories should not depend directly on ActiveMQ client APIs. They should only decide whether to use synchronous DB persistence or publish a workflow log event through the abstraction.

## Write Routing Rules

1. If async workflow log publishing is disabled, keep the existing synchronous DB write behavior.
2. If the workflow invocation is console debugging, keep synchronous DB writes for both workflow runs and node executions.
3. Otherwise, publish log events to ActiveMQ.
4. If publishing fails, log the failure and drop the log event. Do not raise the error to the workflow execution path and do not fall back to synchronous DB writes.

This means Service API, WebApp, and Installed App production invocations can use asynchronous log publishing, while console debugging remains immediately queryable from the database.

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

Repository factories or app generators should derive this from `invoke_from` / workflow run trigger once, then pass it to both repositories. To preserve backward compatibility across existing call sites, any new repository constructor parameter for write mode must default to `WorkflowLogWriteMode.SYNC`.

- `InvokeFrom.DEBUGGER` -> `WorkflowLogWriteMode.SYNC`
- single-step debugging -> `WorkflowLogWriteMode.SYNC`
- Service API / WebApp / Installed App with async enabled -> `WorkflowLogWriteMode.ASYNC`
- any invocation with async disabled -> `WorkflowLogWriteMode.SYNC`

This keeps debugging behavior consistent across workflow run and node execution writes.

## Repository Runtime Semantics

Async publishing changes persistence but must not change repository semantics used by the workflow runtime.

### Read-after-write Cache

`WorkflowCycleManager` saves a workflow execution at run start and later reads it from the same repository instance when the run succeeds, partially succeeds, or fails. Node executions are also saved at node start and then read by `get_by_node_execution_id()` when the node succeeds or fails.

Therefore, async `save()` must still update repository in-memory cache exactly as the synchronous path does:

- `SQLAlchemyWorkflowExecutionRepository.save()` must cache the `WorkflowRun` DB model after converting the domain model, even if it publishes instead of committing.
- `SQLAlchemyWorkflowNodeExecutionRepository.save()` must cache the `WorkflowNodeExecutionModel` by `node_execution_id`, even if it publishes instead of committing.
- `get()` and `get_by_node_execution_id()` must continue checking cache before querying DB.

Within one workflow execution, repository read-after-write behavior must remain identical to the current synchronous implementation.

### Log Query Consistency

Console debugging and single-step debugging remain synchronously persisted and immediately queryable from existing log APIs.

For non-debugging invocations using async workflow log publishing, existing APIs that create a new repository and query DB directly are eventually consistent. Before the external consumer writes events to DB, these APIs may temporarily return:

- no workflow run record,
- an older workflow run status,
- an empty or incomplete node execution list,
- node executions without the latest terminal status.

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

Workflow tracing is a hard requirement and must continue to work when async workflow log publishing is enabled. Tracing must not depend on the future consumer having already written `workflow_runs` or `workflow_node_executions` rows to DB.

Current tracing integrations can create fresh repositories and query DB by `workflow_run_id` to build node spans. That is unsafe in async mode because the trace task can run before log events are consumed. The implementation must provide a runtime snapshot path:

1. At workflow completion, collect the final `WorkflowExecution` and all same-run `WorkflowNodeExecution` domain objects from the repositories' in-memory state.
2. Pass these snapshots into the workflow trace task or into `WorkflowTraceInfo`.
3. Trace preprocessing must build workflow-level trace data from the provided `WorkflowExecution` snapshot when present, instead of requiring a DB `WorkflowRun` row.
4. Trace providers such as Langfuse, Langsmith, Weave, Opik, and Aliyun must prefer the provided node execution snapshots when present.
5. DB lookups remain as fallback for synchronous/default paths, historical trace tasks, and compatibility.

Required behavior:

- Async log mode must produce complete workflow traces and node spans from runtime snapshots.
- Trace generation must not wait for ActiveMQ consumer database writes.
- Trace generation must not force workflow/node logs back to synchronous DB writes.
- Debugging/default sync paths should keep existing DB fallback behavior.

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
    def publish_workflow_run(self, payload: WorkflowRunLogPayload) -> None: ...
    def publish_node_execution(self, payload: WorkflowNodeExecutionLogPayload) -> None: ...
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
```

Notes:

- `WORKFLOW_LOG_ASYNC_ENABLED=false` preserves existing behavior by default.
- This version should accept only `activemq` as the queue provider when async publishing is enabled.
- ActiveMQ client imports should be lazy or optional so default installations are not broken when async publishing is disabled.
- If async publishing is enabled and the STOMP client dependency is unavailable, startup or first publisher creation should fail clearly instead of silently pretending logs are being published.

## Message Format

Use a stable envelope so the future Go consumer is not coupled to Python ORM internals.

Workflow run event:

```json
{
  "event_id": "uuid",
  "event_type": "workflow_run.upsert",
  "schema_version": 1,
  "created_at": "2026-06-06T00:00:00Z",
  "payload": {
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
    "created_by_role": "end_user",
    "created_by": "user_id",
    "created_at": "2026-06-06T00:00:00Z",
    "finished_at": "2026-06-06T00:00:01Z"
  }
}
```

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
event_type: workflow_run.upsert | workflow_node_execution.upsert
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
  - workflow run transitions should treat `succeeded`, `failed`, `partial-succeeded`, and `stopped` as terminal relative to `running`.
- Send poison messages to a dead-letter path according to the consumer project's policy.

## Publisher Connection Lifecycle

The ActiveMQ publisher should reuse connections instead of creating a new connection per message.

Expected behavior:

- Connection reuse is per API process. Multi-process workers maintain independent connections.
- Publish and reconnect operations must be thread-safe if a publisher instance can be shared by multiple threads.
- Implementations may either protect a shared process connection with a lock or use thread-local connections.
- Lazily create the STOMP connection on first publish.
- Reuse the connection for subsequent publishes in the API process or thread, depending on the chosen thread-safety model.
- Apply a short publish timeout from `WORKFLOW_LOG_PUBLISH_TIMEOUT`.
- On connection or send failure, close/reset the connection and fail-open for that message.
- Allow a later publish to reconnect lazily.
- Do not block workflow execution on long broker reconnect loops.

## Security Notes

Workflow inputs, outputs, process data, and metadata can contain sensitive customer data. ActiveMQ deployment should follow minimum security requirements:

- Use broker credentials; do not allow anonymous publishing in production.
- Prefer TLS/STOMP-over-SSL when traffic crosses hosts or networks.
- Keep the broker on a private network and do not expose it publicly.
- Use a least-privilege broker user scoped to the workflow log queue where possible.
- Treat queue retention, dead-letter queues, and broker backups as sensitive data stores.

## Failure Handling

Publishing failures are fail-open:

- Catch publisher exceptions.
- Log a warning or error with enough context to troubleshoot.
- Increment metrics when available.
- Drop the log event.
- Continue workflow execution normally.
- Do not fall back to synchronous database writes, because fallback can recreate the database overload this feature is meant to avoid.

Recommended future metrics:

- `workflow_log_publish_success_total`
- `workflow_log_publish_failed_total`
- `workflow_log_publish_dropped_total`
- `workflow_log_publish_latency_seconds`

## Testing Strategy

- Unit-test routing decisions:
  - async disabled uses synchronous DB write.
  - workflow run debugging uses synchronous DB write.
  - full console debugging node executions use synchronous DB write even though their node trigger is `WORKFLOW_RUN`.
  - single-step node executions use synchronous DB write.
  - non-debugging with async enabled publishes events.
  - publisher exceptions do not propagate.
- Unit-test repository cache semantics:
  - workflow run async save can be read back by `get()` from the same repository instance.
  - node execution async save can be read back by `get_by_node_execution_id()` from the same repository instance.
  - async `get_running_executions()` returns cached running nodes for workflow failure completion.
- Unit-test tracing snapshot behavior:
  - async workflow trace preprocessing can build workflow trace data without a DB `WorkflowRun` row.
  - trace providers prefer provided node execution snapshots over DB lookups.
  - node spans are still produced when the DB has not yet been updated by the consumer.
- Unit-test payload serialization for workflow run and node execution events, including node `triggered_from`.
- Unit-test ActiveMQ publisher headers, especially `JMSXGroupID`.
- Unit-test provider validation: async enabled with unsupported provider should fail configuration or fall back explicitly according to implementation choice.
- Keep existing repository tests passing for default configuration.

## Compatibility

This design is backward-compatible because async publishing is disabled by default and debugging remains synchronous. Existing deployments without ActiveMQ continue using the current SQLAlchemy persistence path.

## Open Implementation Notes

- Prefer lazy importing the STOMP client in the ActiveMQ publisher so installations without the optional dependency are unaffected unless async publishing is enabled.
- Reuse existing model-to-DB mapping methods where possible to avoid duplicating field conversion logic.
- Add an explicit write-mode constructor parameter rather than inferring node debugging from `WorkflowNodeExecutionTriggeredFrom`; default it to `WorkflowLogWriteMode.SYNC` for backward compatibility.
- Keep cache updates shared between sync and async save paths to preserve workflow runtime behavior.
- Add repository or trace-task accessors for same-run workflow/node execution snapshots so tracing does not depend on async DB writes.
- Keep the publisher interface small and dedicated to workflow logs; do not generalize it into a broad messaging framework in this version.
