import importlib.util
from pathlib import Path

import pytest
import sqlalchemy as sa

MIGRATION = (
    Path(__file__).parents[3]
    / "migrations"
    / "versions"
    / "2026_07_03_1200-a1b2c3d4e5f6_add_workflow_node_execution_state_version.py"
)


def test_migration_adds_nullable_bigint_state_version(monkeypatch: pytest.MonkeyPatch) -> None:
    spec = importlib.util.spec_from_file_location("state_version_migration", MIGRATION)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class FakeBatch:
        column: sa.Column | None = None

        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def add_column(self, column: sa.Column) -> None:
            self.column = column

    batch = FakeBatch()
    monkeypatch.setattr(module.op, "batch_alter_table", lambda *args, **kwargs: batch)

    module.upgrade()

    column = batch.column
    assert column is not None
    assert column.name == "state_version"
    assert isinstance(column.type, sa.BigInteger)
    assert column.nullable is True
