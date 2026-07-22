# Question Classifier Retry Design

## Goal

Add optional retry support to the Question Classifier workflow node by reusing the workflow engine and frontend retry components already used by LLM, HTTP, Tool, and Code nodes.

## Behavior

- Retry is disabled by default for new and existing Question Classifier nodes.
- When enabled, users can configure 1–10 retries and a 100–5000 ms fixed interval through the existing retry panel.
- Any node execution reported as failed is retried by the existing graph engine.
- A successful retry follows the selected classification branch normally.
- After retries are exhausted, the existing failure and error-strategy behavior applies.
- A valid JSON response with a missing or unknown category continues to fall back to the first configured category. This remains a successful execution for backward compatibility.

## Backend

`QuestionClassifierNodeData` already inherits `BaseNodeData`, so it already accepts the shared `retry_config` structure. Add a `retry` property to `QuestionClassifierNode` that returns `node_data.retry_config.retry_enabled`, matching `LLMNode`.

No retry loop, delay, or error handling is added to the node. `ErrorHandler` remains responsible for applying the configured interval, scheduling attempts, emitting retry events, and handling exhausted retries.

## Frontend

Add `BlockEnum.QuestionClassifier` to `hasRetryNode`. This capability check already controls:

- rendering `RetryOnPanel` in the workflow configuration panel;
- rendering `RetryOnNode` on the canvas;
- retry status and details in single-run and workflow-run views.

Do not add a Question Classifier-specific retry component or duplicate LLM/HTTP configuration code. Do not add `retry_config` to the Question Classifier default value: the shared backend default keeps retry disabled, while the existing panel initializes enabled settings to 3 retries and a 1000 ms interval when the user turns it on.

## Compatibility

Workflows without `retry_config` continue to deserialize with retry disabled. Classification inputs, outputs, branches, model invocation, fallback classification, and error strategies remain unchanged.

## Tests

### Backend

Add focused coverage showing that `QuestionClassifierNode.retry` reflects disabled and enabled `retry_config` values. Existing graph engine tests remain responsible for retry counts, delays, and exhausted-retry behavior.

### Frontend

Add focused coverage showing that Question Classifier is recognized as retry-capable. Reuse existing shared panel and retry-log tests rather than duplicating their behavior for this node type.

## Acceptance Criteria

1. A new Question Classifier has retry disabled.
2. Enabling retry exposes the existing max-retries and interval controls.
3. Any failed Question Classifier execution is retried according to the shared configuration.
4. Retry progress and logs appear through the existing shared UI.
5. A successful attempt follows the returned classification branch.
6. Exhausted retries preserve existing failure behavior.
7. Existing workflows behave unchanged unless retry is enabled.
