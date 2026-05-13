# Phoenix Trace Session ID Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let backend workflow/chatflow run requests accept `trace_session_id` and use it as Phoenix/OpenInference `session.id`.

**Architecture:** Keep the field out of workflow inputs and conversation state. Normalize it at API/generator boundaries, pass it through `AppGenerateEntity.extras`, add it to workflow/message trace metadata, and make only the Phoenix provider prefer it over the existing session fallback chain. Nested workflow-as-tool calls inherit the value through graph init params and the existing workflow tool private context path.

**Tech Stack:** Python, Flask-RESTful `reqparse`, Pydantic entities, Dify workflow graph runtime, OpenInference `SpanAttributes`, pytest, MDX API docs.

---

## File Structure

- Modify `api/core/ops/trace_context.py`: add `trace_session_id` normalization/extraction helpers.
- Modify controller files under `api/controllers/service_api/app/`, `api/controllers/web/`, `api/controllers/console/explore/`, and `api/controllers/console/app/workflow.py`: parse optional `trace_session_id`.
- Modify app generators `api/core/app/apps/{workflow,chat,agent_chat,advanced_chat}/app_generator.py`: copy normalized `trace_session_id` into `extras`.
- Modify workflow runtime propagation files `api/core/workflow/graph_engine/entities/graph_init_params.py`, `api/core/workflow/graph_engine/graph_engine.py`, `api/core/workflow/workflow_entry.py`, `api/core/workflow/nodes/base/node.py`, `api/core/workflow/nodes/tool/tool_node.py`, and `api/core/tools/workflow_as_tool/tool.py`: carry inherited trace session IDs into nested workflow tools.
- Modify pipeline and trace files `api/core/workflow/workflow_cycle_manager.py`, `api/core/app/apps/workflow/generate_task_pipeline.py`, `api/core/app/apps/advanced_chat/generate_task_pipeline.py`, `api/core/app/task_pipeline/easy_ui_based_generate_task_pipeline.py`, `api/core/ops/ops_trace_manager.py`, and `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`: include metadata and use Phoenix session override.
- Modify tests `api/tests/unit_tests/core/ops/test_trace_context.py`, `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`, `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`, `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`, and `api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py`.
- Modify API docs `web/app/components/develop/template/template_workflow.en.mdx`, `web/app/components/develop/template/template_chat.en.mdx`, and `web/app/components/develop/template/template_advanced_chat.en.mdx`.

### Task 1: Trace Session Helper

**Files:**
- Modify: `api/core/ops/trace_context.py`
- Test: `api/tests/unit_tests/core/ops/test_trace_context.py`

- [ ] **Step 1: Write failing helper tests**

Add tests that prove valid strings are trimmed, blank/null values are ignored, long values fail, and args extraction returns a private extras dict.

```python
def test_normalize_trace_session_id_accepts_trimmed_string():
    assert normalize_trace_session_id("  external-session  ") == "external-session"


def test_normalize_trace_session_id_ignores_blank_or_none():
    assert normalize_trace_session_id(None) is None
    assert normalize_trace_session_id("   ") is None


def test_normalize_trace_session_id_rejects_non_string_and_long_values():
    with pytest.raises(ValueError):
        normalize_trace_session_id(123)
    with pytest.raises(ValueError):
        normalize_trace_session_id("x" * 513)


def test_extract_trace_session_id_from_args():
    assert extract_trace_session_id_from_args({"trace_session_id": " abc "}) == {"trace_session_id": "abc"}
    assert extract_trace_session_id_from_args({}) == {}
```

- [ ] **Step 2: Run tests to verify failure**

Run: `uv run --project api pytest api/tests/unit_tests/core/ops/test_trace_context.py -q`

Expected: FAIL because `normalize_trace_session_id` and `extract_trace_session_id_from_args` do not exist.

- [ ] **Step 3: Implement helper**

Add to `api/core/ops/trace_context.py`:

```python
MAX_TRACE_SESSION_ID_LENGTH = 512


def normalize_trace_session_id(raw_value: Any) -> str | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, str):
        raise ValueError("trace_session_id must be a string")
    value = raw_value.strip()
    if not value:
        return None
    if len(value) > MAX_TRACE_SESSION_ID_LENGTH:
        raise ValueError("trace_session_id must be 512 characters or fewer")
    return value


def extract_trace_session_id_from_args(args: Mapping[str, Any]) -> dict[str, str]:
    trace_session_id = normalize_trace_session_id(args.get("trace_session_id"))
    return {"trace_session_id": trace_session_id} if trace_session_id else {}
```

- [ ] **Step 4: Verify helper tests pass**

Run: `uv run --project api pytest api/tests/unit_tests/core/ops/test_trace_context.py -q`

Expected: PASS.

### Task 2: Parse And Store Backend Request Field

**Files:**
- Modify: `api/controllers/service_api/app/workflow.py`
- Modify: `api/controllers/service_api/app/completion.py`
- Modify: `api/controllers/web/workflow.py`
- Modify: `api/controllers/web/completion.py`
- Modify: `api/controllers/console/explore/workflow.py`
- Modify: `api/controllers/console/explore/completion.py`
- Modify: `api/controllers/console/app/workflow.py`
- Modify: `api/core/app/apps/workflow/app_generator.py`
- Modify: `api/core/app/apps/chat/app_generator.py`
- Modify: `api/core/app/apps/agent_chat/app_generator.py`
- Modify: `api/core/app/apps/advanced_chat/app_generator.py`

- [ ] **Step 1: Add parser argument to run endpoints**

In each workflow/chat parser, add:

```python
parser.add_argument("trace_session_id", type=str, required=False, location="json")
```

Do this for service API, web app, installed app, and console debugger run endpoints. Do not modify frontend request builders.

- [ ] **Step 2: Add generator extras extraction**

Import the helper where needed:

```python
from core.ops.trace_context import extract_trace_session_id_from_args
```

For workflow generator, change extras construction to:

```python
extras = {
    **extract_parent_trace_context_from_args(args),
    **extract_trace_session_id_from_args(args),
}
```

For chat, agent chat, and advanced chat generators, merge into the existing extras:

```python
extras = {
    "auto_generate_conversation_name": args.get("auto_generate_name", True),
    **extract_trace_session_id_from_args(args),
}
```

Use the existing default value in each generator: advanced chat currently defaults to `False`; chat and agent chat default to `True`.

- [ ] **Step 3: Run targeted import/type checks**

Run: `uv run --project api ruff check api/core/ops/trace_context.py api/core/app/apps/workflow/app_generator.py api/core/app/apps/chat/app_generator.py api/core/app/apps/agent_chat/app_generator.py api/core/app/apps/advanced_chat/app_generator.py`

Expected: `All checks passed!`

### Task 3: Propagate To Workflow Trace Tasks And Nested Tools

**Files:**
- Modify: `api/core/workflow/graph_engine/entities/graph_init_params.py`
- Modify: `api/core/workflow/graph_engine/graph_engine.py`
- Modify: `api/core/workflow/workflow_entry.py`
- Modify: `api/core/workflow/nodes/base/node.py`
- Modify: `api/core/workflow/nodes/tool/tool_node.py`
- Modify: `api/core/tools/workflow_as_tool/tool.py`
- Modify: `api/core/workflow/workflow_cycle_manager.py`
- Modify: `api/core/app/apps/workflow/generate_task_pipeline.py`
- Modify: `api/core/app/apps/advanced_chat/generate_task_pipeline.py`
- Modify: `api/core/app/task_pipeline/easy_ui_based_generate_task_pipeline.py`
- Modify: `api/core/ops/ops_trace_manager.py`
- Test: `api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py`
- Test: `api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py`

- [ ] **Step 1: Extend runtime plumbing**

Add optional `trace_session_id: str | None = None` to `GraphInitParams`, `GraphEngine.__init__`, and `WorkflowEntry.__init__`, then pass it into `GraphInitParams`.

In `BaseNode.__init__`, store:

```python
self.trace_session_id = graph_init_params.trace_session_id
```

When workflow and advanced-chat app runners create `WorkflowEntry`, pass:

```python
trace_session_id=self.application_generate_entity.extras.get("trace_session_id"),
```

- [ ] **Step 2: Extend workflow-as-tool inheritance**

In `WorkflowTool`, add `_trace_session_id: str | None`, copy it in `fork_tool_runtime`, and add:

```python
def set_trace_session_id(self, trace_session_id: str | None) -> None:
    self._trace_session_id = trace_session_id
```

When building `generator_args`, include:

```python
if self._trace_session_id:
    generator_args["trace_session_id"] = self._trace_session_id
```

In `ToolNode._run()`, after parent trace context handling, call:

```python
if hasattr(tool_runtime, "set_trace_session_id"):
    tool_runtime.set_trace_session_id(self.trace_session_id)
```

- [ ] **Step 3: Add trace task metadata**

Add `trace_session_id: str | None = None` to `WorkflowCycleManager.handle_workflow_run_success`, `handle_workflow_run_partial_success`, and `handle_workflow_run_failed`, and pass it into `TraceTask`.

In workflow and advanced-chat generate task pipelines, read once beside `parent_trace_context`:

```python
trace_session_id = self._application_generate_entity.extras.get("trace_session_id")
```

Pass `trace_session_id=trace_session_id` into each workflow cycle manager completion/failure call.

For message traces in `EasyUIBasedGenerateTaskPipeline._save_message`, pass the same extra:

```python
TraceTask(
    TraceTaskName.MESSAGE_TRACE,
    conversation_id=self._conversation_id,
    message_id=self._message_id,
    trace_session_id=self._application_generate_entity.extras.get("trace_session_id"),
)
```

In `OpsTraceManager.workflow_trace()`, write:

```python
trace_session_id = self.kwargs.get("trace_session_id")
if isinstance(trace_session_id, str) and trace_session_id:
    metadata["trace_session_id"] = trace_session_id
```

In message trace preprocessing, add `trace_session_id` to `MessageTraceInfo.metadata` if present.

- [ ] **Step 4: Write and run propagation tests**

Add tests that assert `TraceTask.kwargs["trace_session_id"]` is set by workflow cycle manager, `WorkflowTool` forwards `trace_session_id` into child generator args, and `ToolNode` calls `set_trace_session_id()`.

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py -q
```

Expected: PASS.

### Task 4: Phoenix Provider Session Resolution

**Files:**
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
- Test: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`

- [ ] **Step 1: Write failing Phoenix tests**

Add tests for workflow and message trace custom session behavior:

```python
def test_resolve_session_id_prefers_trace_session_id():
    assert _resolve_session_id(
        trace_session_id="custom-session",
        conversation_id="conversation-id",
        workflow_run_id="workflow-run",
        parent_workflow_run_id="outer-run",
    ) == "custom-session"
```

Add workflow trace assertions that root, workflow, and node spans use `custom-session`. Add a nested workflow assertion that custom session wins while parent carrier is still resolved.

- [ ] **Step 2: Implement resolver change**

Change `_resolve_session_id()` signature and body to:

```python
def _resolve_session_id(
    *,
    trace_session_id: str | None,
    conversation_id: str | None,
    workflow_run_id: str | None,
    parent_workflow_run_id: str | None,
) -> str:
    return trace_session_id or conversation_id or parent_workflow_run_id or workflow_run_id or ""
```

In `workflow_trace()`, read:

```python
trace_session_id = trace_info.metadata.get("trace_session_id")
trace_session_id = trace_session_id if isinstance(trace_session_id, str) and trace_session_id else None
```

Pass it into `_resolve_session_id()`.

For message, LLM message child span, and generate-name spans, set `SpanAttributes.SESSION_ID` to:

```python
trace_info.metadata.get("trace_session_id") or trace_info.message_data.conversation_id
```

- [ ] **Step 3: Run Phoenix tests**

Run: `uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py -q`

Expected: PASS.

### Task 5: API Documentation

**Files:**
- Modify: `web/app/components/develop/template/template_workflow.en.mdx`
- Modify: `web/app/components/develop/template/template_chat.en.mdx`
- Modify: `web/app/components/develop/template/template_advanced_chat.en.mdx`

- [ ] **Step 1: Document optional request field**

Add a request property near `response_mode` / `conversation_id`:

```mdx
<Property name='trace_session_id' type='string' key='trace_session_id'>
  Optional Phoenix tracing session override. This only sets the OpenInference `session.id` exported to Phoenix and does not create or resume a Dify conversation.
</Property>
```

Add `"trace_session_id": "external-session-123"` to the English curl examples for `/workflows/run` and `/chat-messages`.

- [ ] **Step 2: Run markdown grep check**

Run: `rg -n "trace_session_id|Phoenix tracing session" web/app/components/develop/template/template_workflow.en.mdx web/app/components/develop/template/template_chat.en.mdx web/app/components/develop/template/template_advanced_chat.en.mdx`

Expected: each file mentions the new field.

### Task 6: Final Verification

**Files:**
- All modified files.

- [ ] **Step 1: Run focused unit suite**

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py \
  api/tests/unit_tests/core/ops/test_trace_context.py \
  api/tests/unit_tests/core/tools/workflow_as_tool/test_tool.py \
  api/tests/unit_tests/core/workflow/nodes/tool/test_tool_node.py \
  api/tests/unit_tests/core/workflow/test_workflow_cycle_manager.py -q
```

Expected: PASS.

- [ ] **Step 2: Run lint on touched Python files**

Run:

```bash
uv run --project api ruff check \
  api/core/ops/trace_context.py \
  api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py \
  api/core/ops/ops_trace_manager.py \
  api/core/workflow/workflow_cycle_manager.py \
  api/core/workflow/graph_engine/entities/graph_init_params.py \
  api/core/workflow/graph_engine/graph_engine.py \
  api/core/workflow/workflow_entry.py \
  api/core/workflow/nodes/base/node.py \
  api/core/workflow/nodes/tool/tool_node.py \
  api/core/tools/workflow_as_tool/tool.py \
  api/core/app/apps/workflow/app_generator.py \
  api/core/app/apps/chat/app_generator.py \
  api/core/app/apps/agent_chat/app_generator.py \
  api/core/app/apps/advanced_chat/app_generator.py \
  api/core/app/apps/workflow/generate_task_pipeline.py \
  api/core/app/apps/advanced_chat/generate_task_pipeline.py \
  api/core/app/task_pipeline/easy_ui_based_generate_task_pipeline.py
```

Expected: `All checks passed!`

- [ ] **Step 3: Run whitespace check**

Run: `git diff --check`

Expected: no output.
