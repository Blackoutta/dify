"""Lazy registry for providers implemented by the unified tracing path."""

import collections
from typing import TypedDict, override

from core.ops.base_trace_instance import BaseTraceInstance
from core.ops.entities.config_entity import BaseTracingConfig


class UnifiedProviderConfigEntry(TypedDict):
    config_class: type[BaseTracingConfig]
    trace_instance: type[BaseTraceInstance]


class UnifiedTraceProviderConfigMap(collections.UserDict[str, UnifiedProviderConfigEntry]):
    """Resolve unified providers without importing their SDKs until selected."""

    @override
    def __getitem__(self, key: str) -> UnifiedProviderConfigEntry:
        raise KeyError(f"Unified tracing provider is not registered: {key}")


unified_provider_config_map = UnifiedTraceProviderConfigMap()
