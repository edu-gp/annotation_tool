import pytest

from alchemy.admin_server import create_app as create_admin_server_app
from alchemy.annotation_server import create_app as create_annotation_server_app
from alchemy.db.model import db


@pytest.fixture
def admin_server_client(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))
    app = create_admin_server_app()

    with app.app_context():
        db.create_all()

        with app.test_client() as client:
            yield client


@pytest.fixture
def annotation_server_client(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))
    app = create_annotation_server_app()

    with app.app_context():
        db.create_all()

        with app.test_client() as client:
            yield client


@pytest.fixture
def config(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    from pathlib import Path
    from flask import Config as FlaskConfigManager

    config = FlaskConfigManager(Path('.').resolve().absolute())
    if not config.from_envvar('ALCHEMY_CONFIG', silent=True):
        config.from_pyfile('../alchemy/config/test.py')

    return config
