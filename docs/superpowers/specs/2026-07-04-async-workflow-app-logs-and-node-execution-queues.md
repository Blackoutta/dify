# Async Workflow App Logs and Node Execution Queue Split

Date: 2026-07-04

## Goal

Move `workflow_app_logs` writes out of the API request path when async workflow log publishing is enabled, and make queue/config names explicitly distinguish node execution logs from app logs.

Stability is preferred over raw throughput.

## Non-goals

- Do not make `workflow_runs` fully async.
- Do not add another consumer service in Docker Compose.
- Do not preserve compatibility with the old generic queue/config names; this branch will redeploy producer and consumer together.

## Producer behavior in Dify

Reuse the existing async workflow log publishing switch. Do not add a separate app-log async switch.

When async workflow log publishing is disabled:

- Node execution persistence follows the synchronous/non-ActiveMQ path.
- `workflow_app_logs` keeps the current synchronous DB insert path.

When async workflow log publishing is enabled:

- Node execution snapshots publish to the node execution queue.
- Workflow app log creation publishes to the app log queue.
- If publishing an app log fails, synchronously insert the app log as a fallback.

## Queue names

Old generic queue names are removed:

```env
/queue/dify.workflow.logs
/queue/dify.workflow.logs.dlq
```

New defaults:

```env
/queue/dify.workflow.node-executions
/queue/dify.workflow.node-executions.dlq
/queue/dify.workflow.app-logs
/queue/dify.workflow.app-logs.dlq
```

## Producer environment variables

Rename the node execution queue variable so it is explicit:

```env
WORKFLOW_NODE_EXECUTION_ACTIVEMQ_DESTINATION=/queue/dify.workflow.node-executions
WORKFLOW_APP_LOG_ACTIVEMQ_DESTINATION=/queue/dify.workflow.app-logs
```

Remove the old generic producer variable:

```env
WORKFLOW_LOG_ACTIVEMQ_DESTINATION
```

Other existing ActiveMQ producer settings, connection settings, and the existing async workflow log switch remain shared unless implementation requires otherwise.

## Consumer process model

Use one consumer process with two worker groups:

```text
consumer process
├─ node_execution_worker_group
│  ├─ queue: /queue/dify.workflow.node-executions
│  ├─ dlq: /queue/dify.workflow.node-executions.dlq
│  ├─ batch: 250
│  └─ max_in_flight: 5
└─ app_log_worker_group
   ├─ queue: /queue/dify.workflow.app-logs
   ├─ dlq: /queue/dify.workflow.app-logs.dlq
   ├─ batch: 500
   └─ max_in_flight: 2
```

Both worker groups share the same DB pool and process health/metrics. App log defaults stay conservative so app log writes do not starve node execution writes.

## Consumer environment variables

Remove old generic node execution variables:

```env
CONSUMER_QUEUE_DESTINATION
CONSUMER_DLQ_DESTINATION
CONSUMER_BATCH_SIZE
CONSUMER_FLUSH_INTERVAL
CONSUMER_MAX_IN_FLIGHT_BATCHES
```

Add explicit node execution variables:

```env
CONSUMER_NODE_EXECUTION_QUEUE_DESTINATION=/queue/dify.workflow.node-executions
CONSUMER_NODE_EXECUTION_DLQ_DESTINATION=/queue/dify.workflow.node-executions.dlq
CONSUMER_NODE_EXECUTION_BATCH_SIZE=250
CONSUMER_NODE_EXECUTION_FLUSH_INTERVAL=1s
CONSUMER_NODE_EXECUTION_MAX_IN_FLIGHT_BATCHES=5
```

Add app log variables:

```env
CONSUMER_APP_LOG_QUEUE_DESTINATION=/queue/dify.workflow.app-logs
CONSUMER_APP_LOG_DLQ_DESTINATION=/queue/dify.workflow.app-logs.dlq
CONSUMER_APP_LOG_BATCH_SIZE=500
CONSUMER_APP_LOG_FLUSH_INTERVAL=1s
CONSUMER_APP_LOG_MAX_IN_FLIGHT_BATCHES=2
```

Keep shared consumer variables:

```env
CONSUMER_QUEUE_PROVIDER=activemq
CONSUMER_MAX_REDELIVERIES=10
CONSUMER_DLQ_INCLUDE_ORIGINAL_BODY=true
CONSUMER_SHUTDOWN_TIMEOUT=30s
CONSUMER_MAX_MESSAGE_BYTES=0
CONSUMER_MAX_DLQ_BODY_BYTES=0
```

## App log event schema

Publish one event per app log:

```json
{
  "event_id": "uuid",
  "event_type": "workflow_app_log.insert",
  "schema_version": 1,
  "created_at": "2026-07-04T00:00:00Z",
  "payload": {
    "id": "uuid",
    "tenant_id": "uuid",
    "app_id": "uuid",
    "workflow_id": "uuid",
    "workflow_run_id": "uuid",
    "created_from": "service-api",
    "created_by_role": "end_user",
    "created_by": "uuid",
    "created_at": "2026-07-04T00:00:00Z"
  }
}
```

`created_from` values follow `WorkflowAppLogCreatedFrom`.

## Consumer app log write

Batch insert app logs with idempotency:

```sql
insert into workflow_app_logs (
  id, tenant_id, app_id, workflow_id, workflow_run_id,
  created_from, created_by_role, created_by, created_at
)
values (...)
on conflict (id) do nothing
```

Invalid app log messages go to the app log DLQ. Transient DB errors leave messages unacked for redelivery, matching the node execution worker behavior.

## Verification

After implementation and redeploy:

- API load test has `500 = 0`.
- Node execution queue has no sustained backlog.
- App log queue has no sustained backlog.
- `workflow_app_logs` count matches new service API workflow runs for the test window.
- Consumer logs show no DLQ/error spikes.
- Consumer slow SQL for app logs stays materially below node execution slow SQL.
