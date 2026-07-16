import pytest

from core.ops.entities.config_entity import TracingProviderEnum
from core.ops.unified_trace.registry import unified_provider_config_map


def test_registry_exposes_only_implemented_providers():
    phoenix = unified_provider_config_map[TracingProviderEnum.PHOENIX]

    assert phoenix["trace_instance"].__name__ == "UnifiedPhoenixTrace"
    with pytest.raises(KeyError):
        unified_provider_config_map[TracingProviderEnum.LANGSMITH]
