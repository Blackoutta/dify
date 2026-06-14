import pytest
from sqlalchemy import create_engine, text

import core.db.session_factory as session_factory_module
from core.db.session_factory import configure_session_factory, create_session, get_session_maker


def test_create_session_requires_configuration(monkeypatch):
    monkeypatch.setattr(session_factory_module, "_session_maker", None)

    with pytest.raises(RuntimeError, match="Session factory not configured"):
        create_session()


def test_configure_session_factory_creates_working_sessions(monkeypatch):
    monkeypatch.setattr(session_factory_module, "_session_maker", None)
    engine = create_engine("sqlite:///:memory:")

    configure_session_factory(engine, expire_on_commit=False)

    maker = get_session_maker()
    with maker() as session:
        assert session.execute(text("select 1")).scalar_one() == 1
