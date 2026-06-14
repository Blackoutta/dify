from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True, slots=True)
class WorkflowToolRuntimeCacheKey:
    tenant_id: str
    provider_id: str
    tool_name: str


class WorkflowToolRuntimeCache(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    workflow_tools: dict[WorkflowToolRuntimeCacheKey, Any] = Field(default_factory=dict)
