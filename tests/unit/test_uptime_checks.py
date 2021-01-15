import json

import pytest

from alchemy.shared import health_check
from tests.fixtures import admin_server_client, annotation_server_client  # noqa


def _setup_temp_path(monkeypatch, tmp_path):
    from .test_fs_paths import _setup_temp_path as __setup_temp_path

    __setup_temp_path(monkeypatch, tmp_path)
    raw_data_dir = health_check.raw_data_dir(as_path=True)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    return raw_data_dir


def test_file_system_readable(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)
    health_check.get_all_data_files()


def test_file_system_okay(monkeypatch, tmp_path):
    raw_data_dir = _setup_temp_path(monkeypatch, tmp_path)

    # Tests temp directory should be read and writable
    assert health_check.check_file_system()


def test_file_system_deletes_canary_file(monkeypatch, tmp_path):
    raw_data_dir = _setup_temp_path(monkeypatch, tmp_path)

    for f in raw_data_dir.iterdir():
        if not f.is_file():
            continue
        assert "fs-health-check" not in f.name


@pytest.mark.xfail(reason="root user bypasses all the permissions "
                   "and currently the tests are running as root")
def test_file_system_unreadable(monkeypatch, tmp_path):
    raw_data_dir = _setup_temp_path(monkeypatch, tmp_path)

    mode = raw_data_dir.stat().st_mode
    raw_data_dir.chmod(0o500)
    try:
        assert not health_check.check_file_system()
    finally:
        raw_data_dir.chmod(mode)


@pytest.mark.timeout(10)
def test_celery_fails():
    assert not health_check.check_celery()
    # Celery is not available in test environment


@pytest.mark.timeout(10)
@pytest.mark.parametrize("server_name,expected_response", [
    ('annotation_server_client', dict(web='ok')),
    ('admin_server_client', dict(web='ok', celery='error'))
])
def test_status_page(
        annotation_server_client, admin_server_client, server_name,
        expected_response,
):
    server = eval(server_name)
    status = server.get("/status")
    assert status.status == "200 OK"
    response_json = json.loads(status.get_data().decode())
    assert isinstance(response_json, dict)
    assert response_json == expected_response
