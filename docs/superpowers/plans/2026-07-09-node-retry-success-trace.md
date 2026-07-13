# Node Retry Success Trace Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve retry failure details for nodes that later succeed, and show those nodes as retry-succeeded in workflow trace UI without changing their real `status`.

**Architecture:** Store retry history in the existing `WorkflowNodeExecution.process_data.retry_errors` field from the shared `WorkflowPersistenceLayer`, so workflow, advanced-chat/chatflow, RAG pipeline, and every retry-capable node type use the same path. The frontend derives a display-only retry-success state from `status === "succeeded"` plus `process_data.retry_errors`, reusing existing trace panel and warning styles.

**Tech Stack:** Python persistence layer + pytest; Next.js/React/TypeScript + Vitest/React Testing Library; existing workflow i18n in `web/i18n/en-US/workflow.json`.

---

## File Map

- Modify `api/core/app/workflow/layers/persistence.py`
  - Add one module-level key for `retry_errors`.
  - Append retry entries on `NodeRunRetryEvent`.
  - Preserve retry entries when the same node later saves final successful `process_data`.
- Modify `api/tests/unit_tests/core/app/workflow/test_persistence_layer.py`
  - Add one focused test for retry then success.
- Modify `web/app/components/workflow/run/node.tsx`
  - Derive `isRetrySucceeded`.
  - Use warning icon instead of success icon.
  - Show retry-success status and Process Data warning banner.
- Create `web/app/components/workflow/run/__tests__/node.spec.tsx`
  - Cover retry-succeeded rendering for the trace node panel.
- Modify `web/i18n/en-US/workflow.json`
  - Add one i18n key for the Process Data retry banner.

## Task 1: Backend Retry History Persistence

**Files:**
- Modify: `api/core/app/workflow/layers/persistence.py`
- Test: `api/tests/unit_tests/core/app/workflow/test_persistence_layer.py`

- [ ] **Step 1: Write the failing backend test**

Add this test to `TestWorkflowPersistenceLayer` in `api/tests/unit_tests/core/app/workflow/test_persistence_layer.py`:

```python
    def test_retry_errors_are_preserved_after_node_succeeds(self):
        layer, _, node_repo, _ = _make_layer()
        layer._handle_graph_run_started()

        start_event = NodeRunStartedEvent(
            id="exec",
            node_id="node",
            node_type=BuiltinNodeTypes.LLM,
            node_title="LLM",
            start_at=_naive_utc_now(),
        )
        layer._handle_node_started(start_event)

        retry_event = NodeRunRetryEvent(
            id="exec",
            node_id="node",
            node_type=BuiltinNodeTypes.LLM,
            node_title="LLM",
            start_at=_naive_utc_now(),
            error="temporary provider error",
            retry_index=1,
        )
        layer._handle_node_retry(retry_event)

        success_event = NodeRunSucceededEvent(
            id="exec",
            node_id="node",
            node_type=BuiltinNodeTypes.LLM,
            start_at=_naive_utc_now(),
            node_run_result=NodeRunResult(
                inputs={"prompt": "hello"},
                process_data={"model": "test-model"},
                outputs={"text": "ok"},
                metadata={},
            ),
        )
        layer._handle_node_succeeded(success_event)

        saved_execution = node_repo.saved_exec_data[-1]
        assert saved_execution.status == WorkflowNodeExecutionStatus.SUCCEEDED
        assert saved_execution.process_data == {
            "model": "test-model",
            "retry_errors": [
                {
                    "retry_index": 1,
                    "error": "temporary provider error",
                }
            ],
        }
```

- [ ] **Step 2: Run backend test and confirm it fails**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/app/workflow/test_persistence_layer.py::TestWorkflowPersistenceLayer::test_retry_errors_are_preserved_after_node_succeeds -q
```

Expected: FAIL because `retry_errors` is not preserved in final `process_data`.

- [ ] **Step 3: Implement minimal backend persistence**

In `api/core/app/workflow/layers/persistence.py`, add a module-level constant near imports or class-level constants:

```python
RETRY_ERRORS_PROCESS_DATA_KEY = "retry_errors"
```

Add two private helpers to `WorkflowPersistenceLayer` near the existing helper methods:

```python
    def _append_retry_error(self, domain_execution: WorkflowNodeExecution, event: NodeRunRetryEvent) -> None:
        process_data = dict(domain_execution.process_data or {})
        retry_errors = list(process_data.get(RETRY_ERRORS_PROCESS_DATA_KEY) or [])
        retry_errors.append(
            {
                "retry_index": event.retry_index,
                "error": event.error,
            }
        )
        process_data[RETRY_ERRORS_PROCESS_DATA_KEY] = retry_errors
        domain_execution.process_data = process_data

    def _merge_retry_errors(
        self,
        existing_process_data: Mapping[str, Any] | None,
        next_process_data: Mapping[str, Any] | None,
    ) -> Mapping[str, Any] | None:
        existing_retry_errors = (existing_process_data or {}).get(RETRY_ERRORS_PROCESS_DATA_KEY)
        if not existing_retry_errors:
            return next_process_data

        merged_process_data = dict(next_process_data or {})
        merged_process_data[RETRY_ERRORS_PROCESS_DATA_KEY] = existing_retry_errors
        return merged_process_data
```

Update `_handle_node_retry`:

```python
    def _handle_node_retry(self, event: NodeRunRetryEvent) -> None:
        domain_execution = self._get_node_execution(event.id)
        domain_execution.status = WorkflowNodeExecutionStatus.RETRY
        domain_execution.error = event.error
        self._append_retry_error(domain_execution, event)
        self._workflow_node_execution_repository.save(domain_execution)
        self._workflow_node_execution_repository.save_execution_data(domain_execution)
```

Update `_update_node_execution` before `domain_execution.update_from_mapping(...)`:

```python
        if update_outputs:
            process_data = self._merge_retry_errors(domain_execution.process_data, node_result.process_data)
            domain_execution.update_from_mapping(
                inputs=node_result.inputs,
                process_data=process_data,
                outputs=node_result.outputs,
                metadata=node_result.metadata,
            )
```

- [ ] **Step 4: Run backend test and confirm it passes**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/app/workflow/test_persistence_layer.py::TestWorkflowPersistenceLayer::test_retry_errors_are_preserved_after_node_succeeds -q
```

Expected: PASS.

- [ ] **Step 5: Run related backend persistence tests**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/app/workflow/test_persistence_layer.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit backend changes**

```bash
git add api/core/app/workflow/layers/persistence.py api/tests/unit_tests/core/app/workflow/test_persistence_layer.py
git commit -m "feat: preserve retry errors in node process data"
```

## Task 2: Frontend Retry-Succeeded Trace Display

**Files:**
- Modify: `web/app/components/workflow/run/node.tsx`
- Create: `web/app/components/workflow/run/__tests__/node.spec.tsx`
- Modify: `web/i18n/en-US/workflow.json`

- [ ] **Step 1: Add i18n key**

In `web/i18n/en-US/workflow.json`, add this key near the existing `nodes.common.retry.*` keys:

```json
"nodes.common.retry.retryDetectedInProcessData": "Exception retry detected. Details are available in Process Data."
```

- [ ] **Step 2: Write the failing frontend test**

Create `web/app/components/workflow/run/__tests__/node.spec.tsx`:

```tsx
import type { ReactNode } from 'react'
import type { NodeTracing } from '@/types/workflow'
import { fireEvent, render, screen } from '@testing-library/react'
import useTheme from '@/hooks/use-theme'
import { Theme } from '@/types/app'
import { BlockEnum, NodeRunningStatus } from '../../types'
import NodePanel from '../node'

vi.mock('@/hooks/use-theme', () => ({
  default: vi.fn(),
}))

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => key,
  }),
}))

vi.mock('@/context/i18n', () => ({
  useDocLink: () => (path: string) => path,
}))

vi.mock('@/app/components/workflow/nodes/_base/components/editor/code-editor', () => ({
  __esModule: true,
  default: ({ title, value, tip }: { title: ReactNode, value: unknown, tip?: ReactNode }) => (
    <section data-testid="code-editor">
      <div>{title}</div>
      <pre>{JSON.stringify(value)}</pre>
      {tip}
    </section>
  ),
}))

const mockUseTheme = vi.mocked(useTheme)

const createNodeInfo = (overrides: Partial<NodeTracing> = {}): NodeTracing => ({
  id: 'trace-node-1',
  index: 1,
  predecessor_node_id: '',
  node_id: 'node-1',
  node_type: BlockEnum.HttpRequest,
  title: 'HTTP Request',
  inputs: {},
  inputs_truncated: false,
  process_data: {},
  process_data_truncated: false,
  outputs_truncated: false,
  status: NodeRunningStatus.Succeeded,
  elapsed_time: 1,
  metadata: {
    iterator_length: 0,
    iterator_index: 0,
    loop_length: 0,
    loop_index: 0,
  },
  created_at: 0,
  created_by: {
    id: 'user-1',
    name: 'User',
    email: 'user@example.com',
  },
  finished_at: 1,
  ...overrides,
})

describe('NodePanel retry succeeded state', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUseTheme.mockReturnValue({ theme: Theme.light } as ReturnType<typeof useTheme>)
  })

  it('should show retry succeeded warning and process data banner when a succeeded node has retry errors', () => {
    render(
      <NodePanel
        nodeInfo={createNodeInfo({
          process_data: {
            request: 'GET /test HTTP/1.1',
            retry_errors: [
              {
                retry_index: 1,
                error: 'temporary http error',
              },
            ],
          },
        })}
      />,
    )

    fireEvent.click(screen.getByText('HTTP Request'))

    expect(screen.getByText('nodes.common.retry.retrySuccessful')).toBeInTheDocument()
    expect(screen.getByText('nodes.common.retry.retryDetectedInProcessData')).toBeInTheDocument()
    expect(screen.getByTestId('code-editor')).toHaveTextContent('retry_errors')
  })
})
```

- [ ] **Step 3: Run frontend test and confirm it fails**

Run from `web/`:

```bash
pnpm test app/components/workflow/run/__tests__/node.spec.tsx
```

Expected: FAIL because `NodePanel` does not yet render the retry-succeeded label or Process Data banner.

- [ ] **Step 4: Implement minimal frontend display**

In `web/app/components/workflow/run/node.tsx`, add this derived value after the existing node-type derived values:

```tsx
  const retryErrors = Array.isArray(nodeInfo.process_data?.retry_errors) ? nodeInfo.process_data.retry_errors : []
  const isRetrySucceeded = nodeInfo.status === 'succeeded' && retryErrors.length > 0
```

Update the success icon block:

```tsx
          {nodeInfo.status === 'succeeded' && !isRetrySucceeded && (
            <RiCheckboxCircleFill className="ml-2 h-3.5 w-3.5 shrink-0 text-text-success" />
          )}
          {isRetrySucceeded && (
            <RiAlertFill className={cn('ml-2 h-4 w-4 shrink-0 text-text-warning-secondary', inMessage && 'h-3.5 w-3.5')} />
          )}
```

Add the retry-success status block before failed/retry blocks:

```tsx
              {isRetrySucceeded && (
                <StatusContainer status="exception">
                  {t('nodes.common.retry.retrySuccessful', { ns: 'workflow' })}
                </StatusContainer>
              )}
```

Inside the existing `nodeInfo.process_data && (...)` section, render the warning banner immediately before `CodeEditor`:

```tsx
            {nodeInfo.process_data && (
              <div className={cn('mb-1')}>
                {isRetrySucceeded && (
                  <div className="mb-1">
                    <StatusContainer status="exception">
                      {t('nodes.common.retry.retryDetectedInProcessData', { ns: 'workflow' })}
                    </StatusContainer>
                  </div>
                )}
                <CodeEditor
                  readOnly
                  title={<div>{processDataTitle}</div>}
                  language={CodeLanguage.json}
                  value={nodeInfo.process_data}
                  isJSONStringifyBeauty
                />
              </div>
            )}
```

- [ ] **Step 5: Run frontend test and confirm it passes**

Run from `web/`:

```bash
pnpm test app/components/workflow/run/__tests__/node.spec.tsx
```

Expected: PASS.

- [ ] **Step 6: Run related frontend trace tests**

Run from `web/`:

```bash
pnpm test app/components/workflow/run/__tests__/node.spec.tsx app/components/workflow/run/__tests__/tracing-panel.spec.tsx
```

Expected: PASS.

- [ ] **Step 7: Commit frontend changes**

```bash
git add web/app/components/workflow/run/node.tsx web/app/components/workflow/run/__tests__/node.spec.tsx web/i18n/en-US/workflow.json
git commit -m "feat: show retry succeeded trace warning"
```

## Task 3: Final Verification

**Files:**
- Read-only verification unless failures expose a scoped bug.

- [ ] **Step 1: Run targeted backend verification**

Run:

```bash
uv run --project api pytest api/tests/unit_tests/core/app/workflow/test_persistence_layer.py -q
```

Expected: PASS.

- [ ] **Step 2: Run targeted frontend verification**

Run from `web/`:

```bash
pnpm test app/components/workflow/run/__tests__/node.spec.tsx app/components/workflow/run/__tests__/tracing-panel.spec.tsx
```

Expected: PASS.

- [ ] **Step 3: Check changed files**

Run:

```bash
git status --short
```

Expected: only unrelated pre-existing workspace changes remain unstaged.

- [ ] **Step 4: Manual smoke test with local retry server**

Use a workflow/chatflow/RAG pipeline node that supports retry, pointing at:

```text
http://127.0.0.1:18080/test
```

Run enough requests for the local server to return a 500 and then a successful retry. In the historical trace:

- The node row still has `status: "succeeded"`.
- The row icon is a yellow warning icon.
- The expanded node shows "Retry successful".
- The Process Data section shows the warning banner and includes `retry_errors`.
