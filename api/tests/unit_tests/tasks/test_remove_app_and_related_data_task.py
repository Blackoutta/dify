from types import SimpleNamespace

from tasks import remove_app_and_related_data_task as task_module


def test_delete_workflow_tool_providers_invalidates_cache_after_each_commit(monkeypatch):
    invalidated = []
    deleted_ids = []
    commit_count = {"value": 0}

    class FakeQuery:
        def filter(self, *criteria):
            return self

        def delete(self, synchronize_session=False):
            deleted_ids.append("provider-1")

    class FakeSession:
        def query(self, model):
            return FakeQuery()

        def commit(self):
            commit_count["value"] += 1

    class FakeResult:
        rowcount = 1

        def __iter__(self):
            return iter([SimpleNamespace(id="provider-1")])

        def close(self):
            return None

    class EmptyResult:
        rowcount = 0

        def __iter__(self):
            return iter([])

        def close(self):
            return None

    class FakeConnection:
        def __init__(self):
            self.calls = 0

        def execute(self, statement, params):
            self.calls += 1
            if self.calls == 1:
                return FakeResult()
            return EmptyResult()

    class FakeBegin:
        connection = FakeConnection()

        def __enter__(self):
            return self.connection

        def __exit__(self, exc_type, exc, tb):
            return None

    class FakeEngine:
        def begin(self):
            return FakeBegin()

    fake_db = SimpleNamespace(
        session=FakeSession(),
        engine=FakeEngine(),
        text=lambda value: value,
    )

    monkeypatch.setattr(task_module, "db", fake_db)
    monkeypatch.setattr(
        task_module,
        "invalidate_workflow_tool_provider_cache",
        lambda tenant_id, provider_id: invalidated.append((tenant_id, provider_id, commit_count["value"])),
    )

    task_module._delete_workflow_tool_providers("tenant-1", "app-1")

    assert deleted_ids == ["provider-1"]
    assert invalidated == [("tenant-1", "provider-1", 1)]
