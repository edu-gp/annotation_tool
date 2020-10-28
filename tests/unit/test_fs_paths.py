from alchemy.db import fs
from pathlib import Path


def _setup_temp_path(monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))


def test_base_path(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)
    assert fs.filestore_base_dir() == str(tmp_path)

    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', '/tmp/foo/bar')
    assert fs.filestore_base_dir() == '/tmp/foo/bar'

    monkeypatch.delenv('ALCHEMY_FILESTORE_DIR')
    assert fs.filestore_base_dir() == '__filestore'

    monkeypatch.delenv('ALCHEMY_CONFIG')
    assert fs.filestore_base_dir() == '__filestore'


def test_check_base(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)

    _none = fs._check_base(None)
    _empty = fs._check_base('')
    _directory_name = fs._check_base('directory_name')
    for o in (_none, _empty, _directory_name):
        assert isinstance(o, Path)

    assert str(_none) == fs.filestore_base_dir()
    assert str(_empty) != fs.filestore_base_dir()
    assert str(_directory_name) == 'directory_name'


def test_ensure_return_type(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)

    path_str = 'foo/bar/baz'
    path = Path(path_str)

    for p in (path_str, path):
        assert isinstance(fs._ensure_return_type(p, as_path=True), Path)
        assert isinstance(fs._ensure_return_type(p, as_path=False), str)


def test_make_path(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)

    p = 'foo/bar'
    assert fs._make_path(None, False, p) == str(tmp_path / p)
    assert fs._make_path('', False, p) == p
    assert fs._make_path('.', False, p) == p
    assert fs._make_path('./', False, p) == p
    assert fs._make_path('/abs/jazz', False, p) == '/abs/jazz/' + p
    assert fs._make_path(tmp_path / 'fusion', False, p) == str(tmp_path / ('fusion/' + p))


def test_predefined_paths(monkeypatch, tmp_path):
    _setup_temp_path(monkeypatch, tmp_path)

    assert fs.raw_data_dir() == str(tmp_path / fs.RAW_DATA_DIR)
    assert fs.training_data_dir() == str(tmp_path / fs.TRAINING_DATA_DIR)
    assert fs.models_dir() == str(tmp_path / fs.MODELS_DIR)
