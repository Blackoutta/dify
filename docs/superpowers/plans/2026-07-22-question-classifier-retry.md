# Question Classifier Retry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add opt-in retry support to Question Classifier by connecting it to the existing backend retry engine and shared frontend retry UI.

**Architecture:** `QuestionClassifierNodeData` already inherits the shared `retry_config`; the node only needs to expose whether retry is enabled. The frontend capability predicate will identify Question Classifier as retry-capable, which automatically enables the existing panel, canvas status, and run-log integrations.

**Tech Stack:** Python 3, Pydantic, pytest, TypeScript, Jest, React workflow components

---

## File Map

- Modify `api/core/workflow/nodes/question_classifier/question_classifier_node.py`: expose the shared retry flag.
- Modify `api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py`: cover disabled and enabled retry configuration.
- Modify `web/app/components/workflow/utils/node.ts`: declare Question Classifier retry capability.
- Create `web/app/components/workflow/utils/node.spec.ts`: cover the capability predicate.

### Task 1: Enable backend retry capability

**Files:**
- Modify: `api/core/workflow/nodes/question_classifier/question_classifier_node.py`
- Test: `api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py`

- [ ] **Step 1: Add a node factory and failing retry test**

Update the imports and add this focused test to `api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py`:

```python
from unittest import mock

from core.workflow.entities import GraphInitParams
from core.workflow.nodes.llm.file_saver import LLMFileSaver
from core.workflow.nodes.question_classifier import QuestionClassifierNode, QuestionClassifierNodeData


def _create_question_classifier_node(*, retry_enabled: bool) -> QuestionClassifierNode:
    data = {
        "title": "test classifier node",
        "query_variable_selector": ["sys", "query"],
        "model": {
            "provider": "openai",
            "name": "gpt-3.5-turbo",
            "mode": "chat",
            "completion_params": {},
        },
        "classes": [{"id": "1", "name": "class 1"}],
        "retry_config": {
            "retry_enabled": retry_enabled,
            "max_retries": 3,
            "retry_interval": 1000,
        },
    }
    return QuestionClassifierNode(
        id="classifier",
        config={"id": "classifier", "data": data},
        graph_init_params=GraphInitParams(
            tenant_id="tenant",
            app_id="app",
            workflow_id="workflow",
            graph_config={},
            user_id="user",
            user_from="account",
            invoke_from="debugger",
            call_depth=0,
        ),
        graph_runtime_state=mock.MagicMock(),
        llm_file_saver=mock.MagicMock(spec=LLMFileSaver),
    )


def test_question_classifier_retry_reflects_config():
    assert _create_question_classifier_node(retry_enabled=False).retry is False
    assert _create_question_classifier_node(retry_enabled=True).retry is True
```

Keep the existing `ImagePromptMessageContent` import and replace the existing Question Classifier import rather than duplicating it. Also add this assertion to `test_init_question_classifier_node_data` to lock in backward-compatible defaults when `retry_config` is absent:

```python
    assert node_data.retry_config.retry_enabled is False
```

- [ ] **Step 2: Run the backend test and verify it fails**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py -q
```

Expected: `test_question_classifier_retry_reflects_config` fails on the enabled assertion because the inherited `Node.retry` always returns `False`.

- [ ] **Step 3: Add the minimal backend implementation**

Add this property to `QuestionClassifierNode` near its other class-level behavior methods:

```python
    @property
    def retry(self) -> bool:
        return self.node_data.retry_config.retry_enabled
```

Do not add a retry loop or alter `_run`; `api/core/workflow/graph_engine/error_handler.py` already handles all failed node results.

- [ ] **Step 4: Run focused backend verification**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py -q
uv run --project api ruff check api/core/workflow/nodes/question_classifier/question_classifier_node.py api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py
```

Expected: all Question Classifier tests pass and Ruff reports no errors.

- [ ] **Step 5: Commit the backend change**

```bash
git add api/core/workflow/nodes/question_classifier/question_classifier_node.py api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py
git commit -m "feat: enable question classifier retries"
```

### Task 2: Enable the shared frontend retry UI

**Files:**
- Modify: `web/app/components/workflow/utils/node.ts`
- Create: `web/app/components/workflow/utils/node.spec.ts`

- [ ] **Step 1: Write the failing capability test**

Create `web/app/components/workflow/utils/node.spec.ts`:

```typescript
import { BlockEnum } from '@/app/components/workflow/types'
import { hasRetryNode } from './node'

describe('hasRetryNode', () => {
  it('supports question classifier nodes', () => {
    expect(hasRetryNode(BlockEnum.QuestionClassifier)).toBe(true)
  })

  it('does not support unrelated nodes', () => {
    expect(hasRetryNode(BlockEnum.Start)).toBe(false)
  })
})
```

- [ ] **Step 2: Run the frontend test and verify it fails**

Run:

```bash
pnpm --dir web test -- --runInBand app/components/workflow/utils/node.spec.ts
```

Expected: the Question Classifier assertion fails with `Expected: true, Received: false`; the unrelated-node assertion passes.

- [ ] **Step 3: Extend the existing capability predicate**

Update `hasRetryNode` in `web/app/components/workflow/utils/node.ts` without adding a new component:

```typescript
export const hasRetryNode = (nodeType?: BlockEnum) => {
  return nodeType === BlockEnum.LLM
    || nodeType === BlockEnum.Tool
    || nodeType === BlockEnum.HttpRequest
    || nodeType === BlockEnum.Code
    || nodeType === BlockEnum.QuestionClassifier
}
```

This single predicate is already consumed by the workflow panel, canvas node, single-run result, and retry-log views.

- [ ] **Step 4: Run focused frontend verification**

Run:

```bash
pnpm --dir web test -- --runInBand app/components/workflow/utils/node.spec.ts app/components/workflow/nodes/_base/components/workflow-panel/index.spec.tsx
pnpm --dir web eslint app/components/workflow/utils/node.ts app/components/workflow/utils/node.spec.ts --no-cache
```

Expected: both Jest suites pass and ESLint reports no errors.

- [ ] **Step 5: Commit the frontend change**

```bash
git add web/app/components/workflow/utils/node.ts web/app/components/workflow/utils/node.spec.ts
git commit -m "feat: show retry controls for question classifier"
```

### Task 3: Final regression verification

**Files:**
- Verify only; no additional files should change.

- [ ] **Step 1: Run backend regression checks**

```bash
uv run --project api pytest api/tests/unit_tests/core/workflow/nodes/test_question_classifier_node.py api/tests/unit_tests/core/workflow/graph_engine -q
```

Expected: all selected backend tests pass.

- [ ] **Step 2: Run frontend retry regression checks**

```bash
pnpm --dir web test -- --runInBand app/components/workflow/utils/node.spec.ts app/components/workflow/run/utils/format-log/retry/index.spec.ts app/components/workflow/nodes/_base/components/workflow-panel/index.spec.tsx
```

Expected: all selected frontend suites pass.

- [ ] **Step 3: Inspect the final diff and repository state**

```bash
git diff 1.11.1...HEAD --check
git status --short --branch
```

Expected: `git diff --check` exits successfully and the branch has no uncommitted changes. Confirm the diff contains only the design/plan documents, one backend property and test, and one frontend predicate update and test.
