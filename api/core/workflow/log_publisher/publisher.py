from __future__ import annotations

from typing import Protocol

from core.workflow.log_publisher.entities import WorkflowLogEvent


class WorkflowLogPublisher(Protocol):
    def publish(self, event: WorkflowLogEvent) -> None: ...

    def warm_up(self) -> None: ...

    def close(self) -> None: ...


class NoopWorkflowLogPublisher:
    def publish(self, event: WorkflowLogEvent) -> None:
        return None

    def warm_up(self) -> None:
        return None

    def close(self) -> None:
        return None
