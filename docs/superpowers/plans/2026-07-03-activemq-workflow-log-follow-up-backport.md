# ActiveMQ Workflow Log Follow-Up Backport Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fill the confirmed Dify-side gaps in the 1.13.3 ActiveMQ workflow node log backport: ops trace node snapshots, `stomp.py` producer, heartbeat, connection reuse, pooling, warm-up, and slow publish logging.

**Architecture:** Keep workflow runs synchronous. For async APP_RUN node logs, pass node execution snapshots from the workflow persistence layer into ops trace tasks so trace providers do not race the consumer. Replace the one-shot stdlib socket publisher with the source feature's `stomp.py`-style persistent pooled producer, but keep it scoped to workflow node execution events.

**Tech Stack:** Python, Flask app extensions, Dify GraphEngine persistence layer, Pydantic settings, `stomp.py`, ActiveMQ STOMP, pytest.

---

## Files

- Modify: `api/pyproject.toml`
- Modify: `api/uv.lock`
- Modify: `api/.env.example`
- Modify: `api/configs/feature/__init__.py`
- Modify: `api/core/repositories/workflow_node_execution_activemq_repository.py`
- Modify: `api/core/app/workflow/layers/persistence.py`
- Modify: `api/core/ops/entities/trace_entity.py`
- Create: `api/core/ops/workflow_trace_snapshots.py`
- Modify: `api/core/ops/langfuse_trace/langfuse_trace.py`
- Modify: `api/core/ops/langsmith_trace/langsmith_trace.py`
- Modify: `api/core/ops/opik_trace/opik_trace.py`
- Modify: `api/core/ops/weave_trace/weave_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
- Modify: `api/core/ops/aliyun_trace/aliyun_trace.py`
- Modify: `api/core/ops/tencent_trace/tencent_trace.py`
- Create: `api/extensions/ext_workflow_log_publisher.py`
- Modify: `api/app_factory.py`
- Modify: `docker/docker-compose.middleware.yaml`
- Modify: `docker/middleware.env.example`
- Modify tests under `api/tests/unit_tests/core/repositories/`, `api/tests/unit_tests/core/ops/`, `api/tests/unit_tests/core/app/workflow/layers/`, `api/tests/unit_tests/configs/`, `api/tests/unit_tests/docker/`, and `api/tests/unit_tests/extensions/`.

## Non-Scope

- Do not make workflow run persistence asynchronous.
- Do not add `workflow_snapshot`; only node execution snapshots are needed because workflow runs stay synchronous.
- Do not port consumer offload behavior.
- Do not change debugger, single-step, RAG pipeline, or ops tracing fallback behavior when async node logs are disabled.

### Task 1: Add Config and Dependency Surface

**Files:**
- Modify: `api/pyproject.toml`
- Modify: `api/uv.lock`
- Modify: `api/.env.example`
- Modify: `api/configs/feature/__init__.py`
- Test: `api/tests/unit_tests/configs/test_workflow_log_config.py`

- [ ] **Step 1: Write config test**

Add assertions for:

```python
assert config.WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE == 1
assert config.WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD == 0.5
```

- [ ] **Step 2: Run config test and verify failure**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/configs/test_workflow_log_config.py -v
```

Expected: fails because the two settings do not exist.

- [ ] **Step 3: Add settings and env docs**

Add:

```python
WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE: int = Field(
    default=1,
    description="Number of ActiveMQ STOMP producer connections per API worker process",
)
WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD: float = Field(
    default=0.5,
    description="Log workflow log publish timing when one publish takes at least this many seconds",
)
```

Add to `api/.env.example` near existing workflow log settings:

```dotenv
WORKFLOW_LOG_ACTIVEMQ_POOL_SIZE=1
WORKFLOW_LOG_PUBLISH_SLOW_LOG_THRESHOLD=0.5
```

- [ ] **Step 4: Add `stomp.py`**

Run:

```bash
uv add --project api stomp.py
```

- [ ] **Step 5: Run config test and commit**

Run the command from Step 2. Expected: pass.

Commit:

```bash
git add api/pyproject.toml api/uv.lock api/.env.example api/configs/feature/__init__.py api/tests/unit_tests/configs/test_workflow_log_config.py
git commit -m "feat: configure pooled activemq workflow log publisher"
```

### Task 2: Replace One-Shot Socket Publisher with Pooled STOMP Producer

**Files:**
- Modify: `api/core/repositories/workflow_node_execution_activemq_repository.py`
- Test: `api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py`

- [ ] **Step 1: Write failing producer tests**

Add tests named `test_publisher_reuses_connection_for_successive_publishes`,
`test_publisher_resets_connection_and_retries_on_send_failure`,
`test_publisher_round_robins_pool_slots`, and `test_publisher_logs_slow_publish_timing`.
Patch `stomp.Connection` with fake connection objects; do not require a live broker.

- [ ] **Step 2: Run repository tests and verify failure**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py -v
```

- [ ] **Step 3: Implement minimal pooled publisher**

Inside `workflow_node_execution_activemq_repository.py`:

- Keep `ActiveMQWorkflowNodeExecutionRepository.save()` behavior unchanged.
- Replace socket frame helpers with a `_ConnectionSlot` dataclass.
- `WorkflowNodeExecutionActiveMQPublisher.publish()` should select a slot, lock that slot, ensure a `stomp.Connection`, send JSON, retry by resetting the slot connection, and log slow publishes.
- Add `warm_up()` and `close()`.
- Use `JMSXGroupID` from `workflow_run_id` or `id`.

- [ ] **Step 4: Run repository tests and commit**

Run the command from Step 2. Expected: pass.

Commit:

```bash
git add api/core/repositories/workflow_node_execution_activemq_repository.py api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py
git commit -m "feat: pool activemq workflow node log producers"
```

### Task 3: Share and Warm Up the Producer

**Files:**
- Modify: `api/core/repositories/workflow_node_execution_activemq_repository.py`
- Create: `api/extensions/ext_workflow_log_publisher.py`
- Modify: `api/app_factory.py`
- Test: `api/tests/unit_tests/extensions/test_ext_workflow_log_publisher.py`

- [ ] **Step 1: Write extension tests**

Add tests named `test_workflow_log_publisher_extension_skips_when_disabled` and
`test_workflow_log_publisher_extension_warms_when_enabled`.

- [ ] **Step 2: Run extension tests and verify failure**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/extensions/test_ext_workflow_log_publisher.py -v
```

- [ ] **Step 3: Add singleton producer helper and extension**

Add a module-level `get_workflow_node_execution_activemq_publisher()` keyed by config. Register `close()` with `atexit`.

Create `api/extensions/ext_workflow_log_publisher.py`:

```python
import logging

from configs import dify_config
from core.repositories.workflow_node_execution_activemq_repository import (
    get_workflow_node_execution_activemq_publisher,
)

logger = logging.getLogger(__name__)


def is_enabled() -> bool:
    return bool(dify_config.WORKFLOW_LOG_ASYNC_ENABLED) and str(dify_config.WORKFLOW_LOG_QUEUE_PROVIDER).lower() == "activemq"


def init_app(app):
    if not is_enabled():
        return
    try:
        get_workflow_node_execution_activemq_publisher().warm_up()
        logger.info("Warmed up workflow node execution ActiveMQ publisher")
    except Exception:
        logger.warning("Failed to warm up workflow node execution ActiveMQ publisher", exc_info=True)
```

Wire it from `api/app_factory.py` with the other optional extensions.

- [ ] **Step 4: Run extension and repository tests, then commit**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/extensions/test_ext_workflow_log_publisher.py api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py -v
```

Commit:

```bash
git add api/core/repositories/workflow_node_execution_activemq_repository.py api/extensions/ext_workflow_log_publisher.py api/app_factory.py api/tests/unit_tests/extensions/test_ext_workflow_log_publisher.py
git commit -m "feat: warm activemq workflow log producer"
```

### Task 4: Add Ops Trace Node Snapshots

**Files:**
- Modify: `api/core/ops/entities/trace_entity.py`
- Create: `api/core/ops/workflow_trace_snapshots.py`
- Modify: `api/core/repositories/workflow_node_execution_activemq_repository.py`
- Modify: `api/core/app/workflow/layers/persistence.py`
- Test: `api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py`
- Test: `api/tests/unit_tests/core/app/workflow/layers/test_persistence.py`

- [ ] **Step 1: Write snapshot utility tests**

Add `test_workflow_node_executions_from_snapshots_returns_none_without_snapshots` and
`test_workflow_node_snapshot_to_domain_like_coerces_provider_fields`.
Cover status, node type, datetime, metadata key coercion, and empty snapshot fallback.

- [ ] **Step 2: Write repository snapshot test**

Assert that after `save(execution)`, the ActiveMQ repository can return cached executions and a JSON-safe trace snapshot for that execution.

- [ ] **Step 3: Write persistence trace enqueue test**

Assert `_enqueue_trace_task()` passes `node_execution_snapshots` when the repository exposes cache/snapshot methods.

- [ ] **Step 4: Run tests and verify failure**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py -v
```

- [ ] **Step 5: Implement node snapshot support**

Add `node_execution_snapshots: list[dict[str, Any]] = []` to `WorkflowTraceInfo`.

Add `workflow_node_snapshot_to_domain_like()` and `workflow_node_executions_from_snapshots()` in `api/core/ops/workflow_trace_snapshots.py`.

Add methods to `ActiveMQWorkflowNodeExecutionRepository`:

```python
def get_cached_executions_by_workflow_run(self, workflow_run_id: str) -> Sequence[WorkflowNodeExecution]:
    return self.get_by_workflow_run(workflow_run_id)

def to_trace_snapshot(self, execution: WorkflowNodeExecution) -> dict[str, Any]:
    converter = WorkflowRuntimeTypeConverter()
    return {
        "id": execution.id,
        "tenant_id": self._tenant_id,
        "app_id": self._app_id,
        "workflow_id": execution.workflow_id,
        "workflow_run_id": execution.workflow_execution_id,
        "node_execution_id": execution.node_execution_id,
        "node_id": execution.node_id,
        "node_type": _value(execution.node_type),
        "title": execution.title,
        "inputs": converter.to_json_encodable(execution.inputs),
        "process_data": converter.to_json_encodable(execution.process_data),
        "outputs": converter.to_json_encodable(execution.outputs),
        "status": _value(execution.status),
        "error": execution.error,
        "elapsed_time": execution.elapsed_time,
        "metadata": jsonable_encoder(execution.metadata or {}),
        "created_at": _iso_utc(execution.created_at),
        "finished_at": _iso_utc(execution.finished_at),
    }
```

In `WorkflowPersistenceLayer._enqueue_trace_task()`, pass `node_execution_snapshots` only when both repository methods exist.

- [ ] **Step 6: Run tests and commit**

Run the command from Step 4. Expected: pass.

Commit:

```bash
git add api/core/ops/entities/trace_entity.py api/core/ops/workflow_trace_snapshots.py api/core/repositories/workflow_node_execution_activemq_repository.py api/core/app/workflow/layers/persistence.py api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py
git commit -m "feat: pass workflow node snapshots to ops traces"
```

### Task 5: Make Trace Providers Prefer Snapshots

**Files:**
- Modify: `api/core/ops/langfuse_trace/langfuse_trace.py`
- Modify: `api/core/ops/langsmith_trace/langsmith_trace.py`
- Modify: `api/core/ops/opik_trace/opik_trace.py`
- Modify: `api/core/ops/weave_trace/weave_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
- Modify: `api/core/ops/aliyun_trace/aliyun_trace.py`
- Modify: `api/core/ops/tencent_trace/tencent_trace.py`
- Test: provider-focused tests under `api/tests/unit_tests/core/ops/`

- [ ] **Step 1: Write one focused provider test**

Use the provider with the smallest existing fixture surface. Assert a `WorkflowTraceInfo` with one `node_execution_snapshots` entry does not call `DifyCoreRepositoryFactory.create_workflow_node_execution_repository`.

- [ ] **Step 2: Run provider test and verify failure**

Run that focused test with:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py -v
```

- [ ] **Step 3: Add snapshot-first helper calls**

In every provider that currently calls `get_by_workflow_run()`, add:

```python
from core.ops.workflow_trace_snapshots import workflow_node_executions_from_snapshots

workflow_node_executions = workflow_node_executions_from_snapshots(trace_info)
if workflow_node_executions is None:
    # existing DB repository fallback
```

Keep existing DB fallback unchanged.

- [ ] **Step 4: Run provider tests and commit**

Run the focused provider tests. Expected: pass.

Commit:

```bash
git add api/core/ops/*_trace api/tests/unit_tests/core/ops
git commit -m "fix: use workflow node snapshots for ops traces"
```

### Task 6: Add ActiveMQ STOMP Heartbeat to Middleware

**Files:**
- Modify: `docker/docker-compose.middleware.yaml`
- Modify: `docker/middleware.env.example`
- Test: `api/tests/unit_tests/docker/test_middleware_compose_activemq.py`

- [ ] **Step 1: Write heartbeat compose test**

Assert:

```python
assert "transport.defaultHeartBeat=${ACTIVEMQ_STOMP_DEFAULT_HEARTBEAT:-30000,30000}" in command
assert "transport.hbGracePeriodMultiplier=${ACTIVEMQ_STOMP_HB_GRACE_PERIOD_MULTIPLIER:-2.0}" in command
assert "exec /opt/apache-activemq/bin/activemq console" in command
```

- [ ] **Step 2: Run docker test and verify failure**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/docker/test_middleware_compose_activemq.py -v
```

- [ ] **Step 3: Add compose command and env defaults**

Add source-compatible command to patch the STOMP transport URI and start ActiveMQ. Add:

```dotenv
ACTIVEMQ_STOMP_DEFAULT_HEARTBEAT=30000,30000
ACTIVEMQ_STOMP_HB_GRACE_PERIOD_MULTIPLIER=2.0
```

- [ ] **Step 4: Run docker test and compose render, then commit**

Run:

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests api/tests/unit_tests/docker/test_middleware_compose_activemq.py -v
cd docker && docker compose -f docker-compose.middleware.yaml --env-file middleware.env config activemq
```

Commit:

```bash
git add docker/docker-compose.middleware.yaml docker/middleware.env.example api/tests/unit_tests/docker/test_middleware_compose_activemq.py
git commit -m "fix: configure activemq stomp heartbeat"
```

### Task 7: Focused Verification

- [ ] **Run Dify focused tests**

```bash
uv run --project api pytest -o addopts='' --confcutdir=api/tests/unit_tests \
  api/tests/unit_tests/configs/test_workflow_log_config.py \
  api/tests/unit_tests/core/repositories/test_workflow_node_execution_activemq_repository.py \
  api/tests/unit_tests/core/repositories/test_factory_workflow_node_execution_async.py \
  api/tests/unit_tests/core/ops/test_workflow_trace_snapshots.py \
  api/tests/unit_tests/core/app/workflow/layers/test_persistence.py \
  api/tests/unit_tests/docker/test_middleware_compose_activemq.py \
  api/tests/unit_tests/extensions/test_ext_workflow_log_publisher.py \
  -v
```

- [ ] **Run local broker smoke test**

With middleware running, publish one workflow node execution event through the repository publisher and verify the command exits with `published`.

- [ ] **Final commit if verification-only fixes were needed**

Run `git status --short`. If verification required code fixes, commit only those files with `git commit -m "test: cover activemq workflow log follow ups"`.
