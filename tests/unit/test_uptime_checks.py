import pytest

from alchemy.shared import health_check
from .test_fs_paths import _setup_temp_path


def test_file_system_readable(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)
    raw_data_dir = health_check.raw_data_dir(as_path=True)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    health_check.get_all_data_files()


def test_file_system_okay(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)
    raw_data_dir = health_check.raw_data_dir(as_path=True)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    # Tests temp directory should be read and writable
    assert health_check.check_file_system()


def test_file_system_deletes_canary_file(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)
    raw_data_dir = health_check.raw_data_dir(as_path=True)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    for f in raw_data_dir.iterdir():
        if not f.is_file():
            continue
        assert "fs-health-check" not in f.name


@pytest.mark.xfail(reason="root user bypasses all the permissions "
                   "and currently the tests are running as root")
def test_file_system_unreadable(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)

    raw_data_dir = health_check.raw_data_dir(as_path=True)
    raw_data_dir.mkdir(parents=True, exist_ok=True)

    mode = raw_data_dir.stat().st_mode
    raw_data_dir.chmod(0o500)
    assert not health_check.check_file_system()
    raw_data_dir.chmod(mode)


@pytest.mark.timeout(10)
def test_celery_fails():
    assert not health_check.check_celery()
    # Celery is not available in test environment
