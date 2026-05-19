# Phoenix Loop Index Wrapper Spans Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add display-only Phoenix wrapper spans that group loop and iteration body node spans by `loop_index` and `iteration_index`.

**Architecture:** Keep the change inside the Phoenix/Arize exporter. Precompute wrapper groups from workflow node execution metadata before emitting node spans, create synthetic wrapper spans under the real loop/iteration container spans, and parent grouped child spans to those wrappers. Tool spans still publish nested workflow parent carriers from the tool span itself.

**Tech Stack:** Python 3.12, pytest, OpenTelemetry SDK, OpenInference semantic conventions, Dify workflow node execution metadata.

---

## Files

- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`
  - Add wrapper index validation and grouping helpers.
  - Add synthetic wrapper span creation in `ArizePhoenixDataTrace.workflow_trace()`.
  - Add `dify.node.loop_index` and `dify.node.iteration_index` child node attributes.
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
  - Add focused helper tests for index normalization and grouping fallback.
  - Update loop body grouping tests.
  - Add iteration grouping, carrier preservation, status/time bounds, and child index attribute tests.

---

### Task 1: Wrapper Group Helper Tests

**Files:**
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`

- [ ] **Step 1: Write failing tests for wrapper index normalization**

Add imports near the existing helper imports:

```python
from core.ops.arize_phoenix_trace.arize_phoenix_trace import (
    ArizePhoenixDataTrace,
    _app_uses_phoenix_provider,
    _build_parent_span_bridge_id,
    _normalize_wrapper_index,
    _parent_workflow_can_publish_span_context,
    _resolve_node_parent_span,
    _resolve_published_parent_span_context,
    _resolve_session_id,
    datetime_to_nanos,
)
```

Add tests near the simple helper tests:

```python
def test_normalize_wrapper_index_accepts_stable_values():
    assert _normalize_wrapper_index(0) == "0"
    assert _normalize_wrapper_index(12) == "12"
    assert _normalize_wrapper_index("01") == "01"
    assert _normalize_wrapper_index("branch-1_A.2:3") == "branch-1_A.2:3"


@pytest.mark.parametrize(
    "value",
    [
        True,
        False,
        -1,
        1.0,
        "",
        " 1",
        "1 ",
        "group/1",
        "group]1",
        None,
    ],
)
def test_normalize_wrapper_index_rejects_unstable_values(value):
    assert _normalize_wrapper_index(value) is None
```

- [ ] **Step 2: Run helper tests and verify they fail because the helper does not exist**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_normalize_wrapper_index_accepts_stable_values api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_normalize_wrapper_index_rejects_unstable_values -q
```

Expected: FAIL during import with `ImportError` or `AttributeError` for `_normalize_wrapper_index`.

- [ ] **Step 3: Implement index normalization helper**

In `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`, add this import near the existing imports:

```python
from dataclasses import dataclass, field
```

Add this constant near `_TRACEPARENT_PATTERN`:

```python
_WRAPPER_INDEX_PATTERN = re.compile(r"^[A-Za-z0-9_.:-]+$")
```

Add this helper near `_build_execution_id_by_node_id()`:

```python
def _normalize_wrapper_index(value: Any) -> str | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return str(value) if value >= 0 else None
    if isinstance(value, str) and _WRAPPER_INDEX_PATTERN.fullmatch(value):
        return value
    return None
```

- [ ] **Step 4: Run helper tests and verify they pass**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_normalize_wrapper_index_accepts_stable_values api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_normalize_wrapper_index_rejects_unstable_values -q
```

Expected: PASS.

---

### Task 2: Precompute Wrapper Groups

**Files:**
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`

- [ ] **Step 1: Write failing tests for loop wrapper grouping and ambiguous fallback**

Add imports:

```python
from core.ops.arize_phoenix_trace.arize_phoenix_trace import (
    ArizePhoenixDataTrace,
    _app_uses_phoenix_provider,
    _build_parent_span_bridge_id,
    _build_wrapper_groups,
    _normalize_wrapper_index,
    _parent_workflow_can_publish_span_context,
    _resolve_node_parent_span,
    _resolve_published_parent_span_context,
    _resolve_session_id,
    datetime_to_nanos,
)
```

Add tests after `test_workflow_trace_keeps_sequential_nodes_as_workflow_children`:

```python
def test_build_wrapper_groups_groups_loop_children_by_index():
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    first = _make_node_execution(
        id="template-row-id-0",
        node_execution_id="template-exec-id-0",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    second = _make_node_execution(
        id="template-row-id-1",
        node_execution_id="template-exec-id-1",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 1}),
    )

    groups = _build_wrapper_groups([loop, first, second])

    assert [(group.key.wrapper_type, group.key.index) for group in groups.values()] == [
        ("loop", "0"),
        ("loop", "1"),
    ]
    assert [group.container_execution_id for group in groups.values()] == ["loop-row-id", "loop-row-id"]
    assert [group.child_execution_ids for group in groups.values()] == [
        {"template-row-id-0"},
        {"template-row-id-1"},
    ]


def test_build_wrapper_groups_skips_ambiguous_container_graph_ids():
    first_loop = _make_node_execution(
        id="loop-row-id-1",
        node_execution_id="loop-exec-id-1",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    second_loop = _make_node_execution(
        id="loop-row-id-2",
        node_execution_id="loop-exec-id-2",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    child = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )

    assert _build_wrapper_groups([first_loop, second_loop, child]) == {}
```

- [ ] **Step 2: Run grouping tests and verify they fail because the helper does not exist**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_build_wrapper_groups_groups_loop_children_by_index api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_build_wrapper_groups_skips_ambiguous_container_graph_ids -q
```

Expected: FAIL during import with `ImportError` or `AttributeError` for `_build_wrapper_groups`.

- [ ] **Step 3: Implement wrapper group data structures and grouping helper**

Add these dataclasses near `_build_execution_id_by_node_id()`:

```python
@dataclass(frozen=True)
class _WrapperGroupKey:
    wrapper_type: str
    container_execution_id: str
    index: str


@dataclass
class _WrapperGroup:
    key: _WrapperGroupKey
    container_execution_id: str
    child_execution_ids: set[str] = field(default_factory=set)
    start_time: datetime | None = None
    end_time: datetime | None = None
    has_error: bool = False
```

Add these helpers near `_resolve_structured_parent_execution_id()`:

```python
def _node_finished_at(node_execution: Any) -> datetime:
    created_at = getattr(node_execution, "created_at", None) or datetime.now()
    elapsed_time = getattr(node_execution, "elapsed_time", None) or 0.0
    return created_at + timedelta(seconds=elapsed_time)


def _resolve_wrapper_group_key(
    node_execution: Any,
    node_metadata: Mapping[str, Any],
    execution_id_by_node_id: Mapping[str, str],
) -> _WrapperGroupKey | None:
    for wrapper_type, container_key, index_key in (
        ("iteration", "iteration_id", "iteration_index"),
        ("loop", "loop_id", "loop_index"),
    ):
        container_id = node_metadata.get(container_key) or getattr(node_execution, container_key, None)
        if not isinstance(container_id, str) or not container_id:
            continue

        container_execution_id = execution_id_by_node_id.get(container_id)
        if container_execution_id is None or container_execution_id == _get_node_execution_id(node_execution):
            continue

        index = _normalize_wrapper_index(node_metadata.get(index_key))
        if index is None:
            continue

        return _WrapperGroupKey(
            wrapper_type=wrapper_type,
            container_execution_id=container_execution_id,
            index=index,
        )

    return None


def _build_wrapper_groups(node_executions: list[Any]) -> dict[_WrapperGroupKey, _WrapperGroup]:
    execution_id_by_node_id = _build_execution_id_by_node_id(node_executions)
    groups: dict[_WrapperGroupKey, _WrapperGroup] = {}

    for node_execution in node_executions:
        node_metadata = _extract_json_mapping(getattr(node_execution, "execution_metadata", None))
        group_key = _resolve_wrapper_group_key(node_execution, node_metadata, execution_id_by_node_id)
        if group_key is None:
            continue

        group = groups.setdefault(
            group_key,
            _WrapperGroup(key=group_key, container_execution_id=group_key.container_execution_id),
        )
        execution_id = _get_node_execution_id(node_execution)
        group.child_execution_ids.add(execution_id)

        created_at = getattr(node_execution, "created_at", None) or datetime.now()
        finished_at = _node_finished_at(node_execution)
        group.start_time = created_at if group.start_time is None else min(group.start_time, created_at)
        group.end_time = finished_at if group.end_time is None else max(group.end_time, finished_at)
        group.has_error = group.has_error or getattr(node_execution, "status", None) != "succeeded"

    return groups
```

- [ ] **Step 4: Run grouping tests and verify they pass**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_build_wrapper_groups_groups_loop_children_by_index api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_build_wrapper_groups_skips_ambiguous_container_graph_ids -q
```

Expected: PASS.

---

### Task 3: Emit Loop Wrapper Spans

**Files:**
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`

- [ ] **Step 1: Replace old repeated loop body flat-parent assertion with failing grouped behavior**

Update `test_workflow_trace_keeps_repeated_loop_body_nodes_under_loop` so each repeated body node includes `loop_index`.

Change the three `execution_metadata=json.dumps({"loop_id": "loop-node"})` calls inside the test loop to:

```python
execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": index}),
```

Change the final assertions to:

```python
    wrapper_spans = [span for span in tracer.spans if span.name.startswith("loop[")]
    assert [span.name for span in wrapper_spans] == ["loop[0]", "loop[1]", "loop[2]"]
    assert {span.parent_name for span in wrapper_spans} == {"loop_main_loop"}

    body_spans = [span for span in tracer.spans if span.name in {
        "template-transform_Template",
        "tool_Embedded_Workflow_2_tool",
        "assigner_Variable_Assigner",
    }]
    assert len(body_spans) == 9
    assert {
        span.attributes["dify.node.execution_id"]: span.parent_name
        for span in body_spans
    } == {
        "template-row-id-0": "loop[0]",
        "tool-row-id-0": "loop[0]",
        "assigner-row-id-0": "loop[0]",
        "template-row-id-1": "loop[1]",
        "tool-row-id-1": "loop[1]",
        "assigner-row-id-1": "loop[1]",
        "template-row-id-2": "loop[2]",
        "tool-row-id-2": "loop[2]",
        "assigner-row-id-2": "loop[2]",
    }
```

Add a no-index regression test after it:

```python
def test_workflow_trace_keeps_loop_body_nodes_under_loop_without_index(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    template = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node"}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, template])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "loop_main_loop",
        "template-transform_Template",
    ]
    assert tracer.spans[3].parent_name == "loop_main_loop"
```

Add a malformed-index regression test after the no-index test:

```python
def test_workflow_trace_ignores_malformed_loop_index(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    template = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": " 1"}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, template])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "loop_main_loop",
        "template-transform_Template",
    ]
    assert tracer.spans[3].parent_name == "loop_main_loop"
```

Add an ambiguous-container export regression test:

```python
def test_workflow_trace_skips_wrapper_when_container_graph_id_is_ambiguous(monkeypatch):
    first_loop = _make_node_execution(
        id="loop-row-id-1",
        node_execution_id="loop-exec-id-1",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    second_loop = _make_node_execution(
        id="loop-row-id-2",
        node_execution_id="loop-exec-id-2",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    child = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[first_loop, second_loop, child])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert "loop[0]" not in [span.name for span in tracer.spans]
    child_span = next(span for span in tracer.spans if span.name == "template-transform_Template")
    assert child_span.parent_name == "Root_Chat_workflow"
```

- [ ] **Step 2: Run loop wrapper test and verify it fails because wrappers are not emitted**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_keeps_repeated_loop_body_nodes_under_loop -q
```

Expected: FAIL because `wrapper_spans` is empty or body spans are still parented to `loop_main_loop`.

- [ ] **Step 3: Implement wrapper span emission and parent selection**

In `workflow_trace()`, after `workflow_nodes = list(...)`, build wrapper groups:

```python
            wrapper_groups = _build_wrapper_groups(workflow_nodes)
            wrapper_span_by_key: dict[_WrapperGroupKey, Any] = {}
            wrapper_key_by_child_execution_id: dict[str, _WrapperGroupKey] = {}
            for group_key, group in wrapper_groups.items():
                for child_execution_id in group.child_execution_ids:
                    wrapper_key_by_child_execution_id[child_execution_id] = group_key
```

Add this nested helper before `emit_node_span()`:

```python
            def emit_wrapper_span(group_key: _WrapperGroupKey) -> Any | None:
                existing_span = wrapper_span_by_key.get(group_key)
                if existing_span is not None:
                    return existing_span

                group = wrapper_groups.get(group_key)
                if group is None:
                    return None

                if group.container_execution_id not in span_by_execution_id:
                    parent_node_execution = node_execution_by_execution_id.get(group.container_execution_id)
                    if parent_node_execution is not None:
                        emit_node_span(parent_node_execution)

                container_span = span_by_execution_id.get(group.container_execution_id)
                if container_span is None:
                    return None

                metadata = {
                    "synthetic": True,
                    "wrapper_type": group_key.wrapper_type,
                    "wrapper_index": group_key.index,
                    "container_execution_id": group.container_execution_id,
                }
                wrapper_span = self.tracer.start_span(
                    name=f"{group_key.wrapper_type}[{group_key.index}]",
                    attributes={
                        SpanAttributes.OPENINFERENCE_SPAN_KIND: OpenInferenceSpanKindValues.CHAIN.value,
                        SpanAttributes.METADATA: json.dumps(metadata, ensure_ascii=False),
                        SpanAttributes.SESSION_ID: session_id,
                        "dify.wrapper.synthetic": True,
                        "dify.wrapper.type": group_key.wrapper_type,
                        "dify.wrapper.index": group_key.index,
                        "dify.wrapper.container_execution_id": group.container_execution_id,
                    },
                    start_time=datetime_to_nanos(group.start_time),
                    context=trace.set_span_in_context(container_span),
                )
                wrapper_span_by_key[group_key] = wrapper_span
                return wrapper_span
```

In `emit_node_span()`, before `node_parent_span = _resolve_node_parent_span_by_execution(...)`, select wrapper parent first:

```python
                wrapper_group_key = wrapper_key_by_child_execution_id.get(execution_id)
                wrapper_parent_span = emit_wrapper_span(wrapper_group_key) if wrapper_group_key else None
                node_parent_span = wrapper_parent_span or _resolve_node_parent_span_by_execution(
                    structured_parent_execution_id=structured_parent_execution_id,
                    span_by_execution_id=span_by_execution_id,
                    workflow_span=workflow_span,
                )
```

Remove the old direct assignment to `node_parent_span` that did not check `wrapper_parent_span`.

After `for node_execution in workflow_nodes: emit_node_span(node_execution)`, end wrapper spans:

```python
            for group_key, wrapper_span in wrapper_span_by_key.items():
                group = wrapper_groups[group_key]
                _set_span_status(wrapper_span, "wrapper child failed" if group.has_error else None)
                wrapper_span.end(end_time=datetime_to_nanos(group.end_time))
```

- [ ] **Step 4: Run loop wrapper and no-index tests**

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_keeps_repeated_loop_body_nodes_under_loop \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_keeps_loop_body_nodes_under_loop_without_index \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_ignores_malformed_loop_index \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_skips_wrapper_when_container_graph_id_is_ambiguous \
  -q
```

Expected: PASS.

---

### Task 4: Iteration Wrappers, Child Index Attributes, and Carrier Preservation

**Files:**
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`

- [ ] **Step 1: Write failing iteration wrapper and child index attribute tests**

Add this test near the loop wrapper tests:

```python
def test_workflow_trace_groups_iteration_body_nodes_by_index(monkeypatch):
    iteration = _make_node_execution(
        id="iteration-row-id",
        node_execution_id="iteration-exec-id",
        node_id="iteration-node",
        title="Iteration",
        node_type="iteration",
        process_data="{}",
    )
    first = _make_node_execution(
        id="if-row-id-0",
        node_execution_id="if-exec-id-0",
        node_id="if-node",
        title="IF/ELSE",
        node_type="if-else",
        process_data="{}",
        execution_metadata=json.dumps({"iteration_id": "iteration-node", "iteration_index": 0}),
    )
    second = _make_node_execution(
        id="template-row-id-1",
        node_execution_id="template-exec-id-1",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"iteration_id": "iteration-node", "iteration_index": 1}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[iteration, first, second])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert [span.name for span in tracer.spans] == [
        "workflow-run-123456",
        "Root_Chat_workflow",
        "iteration_Iteration",
        "iteration[0]",
        "if-else_IF/ELSE_condition",
        "iteration[1]",
        "template-transform_Template",
    ]
    assert tracer.spans[3].parent_name == "iteration_Iteration"
    assert tracer.spans[4].parent_name == "iteration[0]"
    assert tracer.spans[5].parent_name == "iteration_Iteration"
    assert tracer.spans[6].parent_name == "iteration[1]"
    assert tracer.spans[4].attributes["dify.node.iteration_index"] == 0
    assert tracer.spans[6].attributes["dify.node.iteration_index"] == 1
```

Add this test for loop index child attributes:

```python
def test_workflow_trace_exposes_loop_index_as_queryable_node_attribute(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    child = _make_node_execution(
        id="template-row-id",
        node_execution_id="template-exec-id",
        node_id="template-node",
        title="Template",
        node_type="template-transform",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 3}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, child])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    child_span = next(span for span in tracer.spans if span.name == "template-transform_Template")
    assert child_span.attributes["dify.node.loop_index"] == 3
```

Add wrapper semantic attribute assertions to this test:

```python
    wrapper = next(span for span in tracer.spans if span.name == "loop[3]")
    wrapper_metadata = json.loads(wrapper.attributes[SpanAttributes.METADATA])
    assert wrapper.attributes[SpanAttributes.SESSION_ID] == "workflow-run-123456"
    assert wrapper.attributes["dify.wrapper.synthetic"] is True
    assert wrapper.attributes["dify.wrapper.type"] == "loop"
    assert wrapper.attributes["dify.wrapper.index"] == "3"
    assert wrapper.attributes["dify.wrapper.container_execution_id"] == "loop-row-id"
    assert wrapper_metadata == {
        "synthetic": True,
        "wrapper_type": "loop",
        "wrapper_index": "3",
        "container_execution_id": "loop-row-id",
    }
```

- [ ] **Step 2: Run new iteration and attribute tests and verify they fail**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_groups_iteration_body_nodes_by_index api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_exposes_loop_index_as_queryable_node_attribute -q
```

Expected: FAIL because wrapper order or `dify.node.*_index` attributes are missing.

- [ ] **Step 3: Add child index queryable attributes**

In node span attributes, extend the existing `"dify.node"` prefixed attributes:

```python
                                "loop_id": node_metadata.get("loop_id"),
                                "loop_index": node_metadata.get("loop_index"),
                                "iteration_id": node_metadata.get("iteration_id"),
                                "iteration_index": node_metadata.get("iteration_index"),
```

- [ ] **Step 4: Run iteration and attribute tests**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_groups_iteration_body_nodes_by_index api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_exposes_loop_index_as_queryable_node_attribute -q
```

Expected: PASS.

- [ ] **Step 5: Write failing carrier preservation test**

Add this test near `test_workflow_trace_publishes_parent_span_aliases_for_tool_nodes`:

```python
def test_workflow_trace_tool_inside_wrapper_publishes_tool_span_carrier(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
    )
    tool = _make_node_execution(
        id="tool-row-id",
        node_execution_id=None,
        node_id="graph-tool-node",
        title="Call Child",
        node_type="tool",
        process_data="{}",
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    published_contexts = []
    published_keys = []
    instance, _ = _make_trace_instance(monkeypatch, nodes=[loop, tool])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.TraceContextTextMapPropagator",
        lambda: SimpleNamespace(
            inject=lambda carrier, context=None: published_contexts.append(getattr(context, "name", None))
            or carrier.update({"traceparent": "fake"})
        ),
    )
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace._publish_parent_span_context",
        lambda parent_node_execution_id, carrier: published_keys.append(parent_node_execution_id),
    )

    instance.workflow_trace(_make_workflow_trace_info())

    assert published_contexts == ["tool_Call_Child_tool"]
    assert published_keys == [
        "workflow-run-123456:tool-row-id",
        "workflow-run-123456:graph-tool-node",
    ]
```

- [ ] **Step 6: Run carrier preservation test**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_tool_inside_wrapper_publishes_tool_span_carrier -q
```

Expected: PASS after wrapper implementation. If it fails with `loop[0]`, fix the carrier injection site to continue using `node_span` context.

---

### Task 5: Wrapper Time Bounds, Status, and Full Regression

**Files:**
- Modify: `api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py`
- Modify: `api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py`

- [ ] **Step 1: Add wrapper bounds and status regression test**

Add this test near the wrapper tests:

```python
def test_workflow_trace_wrapper_uses_child_time_bounds_and_error_status(monkeypatch):
    loop = _make_node_execution(
        id="loop-row-id",
        node_execution_id="loop-exec-id",
        node_id="loop-node",
        title="Loop",
        node_type="loop",
        process_data="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 0),
        elapsed_time=10.0,
    )
    first = _make_node_execution(
        id="first-row-id",
        node_execution_id="first-exec-id",
        node_id="first-node",
        title="First",
        node_type="template-transform",
        process_data="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 3),
        elapsed_time=2.0,
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    second = _make_node_execution(
        id="second-row-id",
        node_execution_id="second-exec-id",
        node_id="second-node",
        title="Second",
        node_type="template-transform",
        status="failed",
        error="boom",
        process_data="{}",
        created_at=datetime(2026, 1, 1, 0, 0, 4),
        elapsed_time=5.0,
        execution_metadata=json.dumps({"loop_id": "loop-node", "loop_index": 0}),
    )
    instance, tracer = _make_trace_instance(monkeypatch, nodes=[loop, first, second])
    monkeypatch.setattr(
        "core.ops.arize_phoenix_trace.arize_phoenix_trace.trace.set_span_in_context",
        lambda span: span,
    )

    instance.workflow_trace(_make_workflow_trace_info())

    wrapper = next(span for span in tracer.spans if span.name == "loop[0]")
    assert wrapper.start_time == datetime_to_nanos(first.created_at)
    assert wrapper.end_time == datetime_to_nanos(second.created_at + timedelta(seconds=second.elapsed_time))
    assert wrapper.status.status_code == StatusCode.ERROR
```

Add `timedelta` to the datetime import:

```python
from datetime import datetime, timedelta
```

- [ ] **Step 2: Run bounds/status test**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_wrapper_uses_child_time_bounds_and_error_status -q
```

Expected: PASS if Task 3 already handled bounds/status correctly. If it fails on wrapper start/end/status, complete Step 3 before continuing.

- [ ] **Step 3: Fix wrapper bounds/status if needed**

If the test failed, ensure `_build_wrapper_groups()` computes:

```python
        created_at = getattr(node_execution, "created_at", None) or datetime.now()
        finished_at = _node_finished_at(node_execution)
        group.start_time = created_at if group.start_time is None else min(group.start_time, created_at)
        group.end_time = finished_at if group.end_time is None else max(group.end_time, finished_at)
        group.has_error = group.has_error or getattr(node_execution, "status", None) != "succeeded"
```

Ensure wrapper span ending uses:

```python
                _set_span_status(wrapper_span, "wrapper child failed" if group.has_error else None)
                wrapper_span.end(end_time=datetime_to_nanos(group.end_time))
```

- [ ] **Step 4: Run focused wrapper test group**

Run:

```bash
uv run --project api pytest \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_normalize_wrapper_index_accepts_stable_values \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_normalize_wrapper_index_rejects_unstable_values \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_build_wrapper_groups_groups_loop_children_by_index \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_build_wrapper_groups_skips_ambiguous_container_graph_ids \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_keeps_repeated_loop_body_nodes_under_loop \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_keeps_loop_body_nodes_under_loop_without_index \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_ignores_malformed_loop_index \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_skips_wrapper_when_container_graph_id_is_ambiguous \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_groups_iteration_body_nodes_by_index \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_exposes_loop_index_as_queryable_node_attribute \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_tool_inside_wrapper_publishes_tool_span_carrier \
  api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py::test_workflow_trace_wrapper_uses_child_time_bounds_and_error_status \
  -q
```

Expected: PASS.

- [ ] **Step 5: Run full Phoenix trace regression tests**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py api/tests/unit_tests/tasks/test_ops_trace_task.py -q
```

Expected: PASS.

- [ ] **Step 6: Review staged scope and commit implementation**

Run:

```bash
git diff -- api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py
git status --short
```

Expected: implementation changes are limited to the two files listed above, aside from pre-existing unrelated changes in `Makefile` and `docker/middleware.env.example`.

Commit:

```bash
git add api/core/ops/arize_phoenix_trace/arize_phoenix_trace.py api/tests/unit_tests/core/ops/test_arize_phoenix_trace.py
git commit -m "feat: group phoenix loop iterations with wrapper spans"
```

Expected: commit succeeds and pre-existing unrelated files remain unstaged.
