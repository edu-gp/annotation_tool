import pytest
from db.config import TestingConfig
from db.model import db
from admin_server import create_app as create_admin_server_app
from annotation_server import create_app as create_annotation_server_app


@pytest.fixture
def backend_client(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))
    monkeypatch.setenv('ANNOTATION_TOOL_BACKEND_PASSWORD', 'password')

    app = create_admin_server_app(TestingConfig)

    with app.app_context():
        db.create_all()

        with app.test_client() as client:
            yield client


@pytest.fixture
def frontend_client(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))
    monkeypatch.setenv('ANNOTATION_TOOL_FRONTEND_SECRET', 'asdsad')

    app = create_annotation_server_app(TestingConfig)

    with app.app_context():
        db.create_all()

        with app.test_client() as client:
            yield client
