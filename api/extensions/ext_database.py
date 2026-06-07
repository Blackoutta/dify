from core.db.session_factory import session_factory
from dify_app import DifyApp
from models import db


def init_app(app: DifyApp):
    db.init_app(app)
    with app.app_context():
        session_factory.configure(db.engine, expire_on_commit=False)
