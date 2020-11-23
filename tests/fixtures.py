import pytest

from alchemy.admin_server import create_app as create_admin_server_app
from alchemy.annotation_server import create_app as create_annotation_server_app
from alchemy.db.config import TestingConfig
from alchemy.db.model import db


@pytest.fixture
def admin_server_client(monkeypatch, tmp_path):
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    app = create_admin_server_app(TestingConfig)

    with app.app_context():
        db.create_all()

        with app.test_client() as client:
            yield client


@pytest.fixture
def annotation_server_client(monkeypatch, tmp_path):
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))
    monkeypatch.setenv("ANNOTATION_TOOL_ANNOTATION_SERVER_SECRET", "asdsad")

    app = create_annotation_server_app(TestingConfig)

    with app.app_context():
        db.create_all()

        with app.test_client() as client:
            yield client
