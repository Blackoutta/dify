# Fail-fast stopped node message

## Goal

Show the actual upstream failure for nodes stopped by workflow fail-fast instead of displaying `Stop by N/A`.

## Design

Keep the existing `stopped` status and status container. When a stopped trace has `outputs.failed_node_title`, render a translated message containing that title and `outputs.error`. Otherwise preserve the existing manual-stop message based on `created_by.name`.

The frontend consumes the structured outputs already emitted by the workflow engine; no backend schema or synthetic user is added.

## Compatibility

Manual stops continue to display `Stop by {{user}}`. Older stopped traces without fail-fast outputs continue through the existing fallback.

## Verification

Add one `NodePanel` test proving a fail-fast stopped node shows the failed node title and error. Keep the existing rendering path as the compatibility fallback.
