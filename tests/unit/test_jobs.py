from pathlib import Path

import pytest

from alchemy.bg.jobs import export_new_raw_data
from alchemy.db.model import TextClassificationModel
from alchemy.shared.file_adapters import load_jsonl
from tests.utils import create_example_model, assert_file_exists


def test_export_new_raw_data(dbsession, monkeypatch, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = Path('__filestore')
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    ctx = create_example_model(dbsession, cloud=(data_store == 'cloud'))
    model = dbsession.query(TextClassificationModel).first()
    fname = "test_export_new_raw_data.jsonl"
    export_new_raw_data(model, ctx["data_fname"], fname, data_store=data_store)
    expected_output_path = str(tmp_path / "raw_data" / fname)
    assert_file_exists(expected_output_path, local=True, cloud=False)
    df = load_jsonl(expected_output_path, data_store=data_store)
    assert len(df) == 3

    with pytest.raises(Exception) as e:
        export_new_raw_data(model, ctx["data_fname"], fname, data_store=data_store)
        assert "Cannot overwrite existing file" in e
