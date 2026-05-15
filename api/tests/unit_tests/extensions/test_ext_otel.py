import sys
from types import ModuleType

from extensions.ext_otel import _init_redis_instrumentor


def test_init_redis_instrumentor_uses_global_tracer_provider(monkeypatch):
    calls = []

    class FakeRedisInstrumentor:
        def instrument(self, **kwargs):
            calls.append(kwargs)

    fake_module = ModuleType("opentelemetry.instrumentation.redis")
    fake_module.RedisInstrumentor = FakeRedisInstrumentor
    monkeypatch.setitem(sys.modules, "opentelemetry.instrumentation.redis", fake_module)

    tracer_provider = object()

    _init_redis_instrumentor(tracer_provider)

    assert calls == [{"tracer_provider": tracer_provider}]
