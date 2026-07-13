# Fail-fast Stopped Node Message Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `Stop by N/A` with the upstream node failure for traces stopped by fail-fast while preserving manual-stop rendering.

**Architecture:** `NodePanel` already owns stopped-trace rendering and receives the structured failure details in `nodeInfo.outputs`. Add one conditional at that rendering point and one English translation key; do not change backend data or introduce a helper.

**Tech Stack:** React 19, TypeScript, react-i18next, Vitest, React Testing Library

---

### Task 1: Render the fail-fast stop reason

**Files:**
- Modify: `web/app/components/workflow/run/node.tsx:214-218`
- Modify: `web/i18n/en-US/workflow.json:1143`
- Test: `web/app/components/workflow/run/__tests__/node.spec.tsx`

- [ ] **Step 1: Write the failing component test**

Update the local `react-i18next` mock so interpolated values remain observable:

```tsx
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, options?: Record<string, unknown>) => [key, options?.node, options?.error].filter(Boolean).join(' '),
  }),
}))
```

Add one test that opens a fail-fast stopped trace and checks the causal message:

```tsx
it('should show the upstream failure when a node is stopped by fail-fast', () => {
  render(
    <NodePanel
      nodeInfo={createNodeInfo({
        status: NodeRunningStatus.Stopped,
        outputs: {
          failed_node_id: 'failed-node-1',
          failed_node_title: 'HTTP Request (2)',
          error: 'Request failed with status code 500',
        },
      })}
    />,
  )

  fireEvent.click(screen.getByText('HTTP Request'))

  expect(screen.getByText(/tracing\.stopByNode HTTP Request \(2\) Request failed with status code 500/)).toBeInTheDocument()
})
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cd web && pnpm test app/components/workflow/run/__tests__/node.spec.tsx`

Expected: FAIL because `NodePanel` still calls `tracing.stopBy`.

- [ ] **Step 3: Add the minimal rendering branch and translation**

Change the stopped banner body in `node.tsx`:

```tsx
{nodeInfo.outputs?.failed_node_title
  ? t('tracing.stopByNode', {
      ns: 'workflow',
      node: nodeInfo.outputs.failed_node_title,
      error: nodeInfo.outputs.error,
    })
  : t('tracing.stopBy', { ns: 'workflow', user: nodeInfo.created_by ? nodeInfo.created_by.name : 'N/A' })}
```

Add the English translation beside `tracing.stopBy`:

```json
"tracing.stopByNode": "Stopped because {{node}} failed: {{error}}"
```

- [ ] **Step 4: Run focused verification**

Run: `cd web && pnpm test app/components/workflow/run/__tests__/node.spec.tsx`

Expected: PASS.

Run: `cd web && pnpm eslint app/components/workflow/run/node.tsx app/components/workflow/run/__tests__/node.spec.tsx`

Expected: PASS with no errors.

- [ ] **Step 5: Commit the implementation**

```bash
git add web/app/components/workflow/run/node.tsx web/app/components/workflow/run/__tests__/node.spec.tsx web/i18n/en-US/workflow.json
git commit -m "fix: show fail-fast stop reason"
```
