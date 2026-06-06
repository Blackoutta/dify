from __future__ import annotations

from typing import Protocol

from core.workflow.log_publisher.entities import WorkflowLogEvent


class WorkflowLogPublisher(Protocol):
    def publish(self, event: WorkflowLogEvent) -> None: ...


class NoopWorkflowLogPublisher:
    def publish(self, event: WorkflowLogEvent) -> None:
        return None
