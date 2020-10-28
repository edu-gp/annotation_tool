import os

import pytest

from alchemy.db.model import TextClassificationModel
from tests.utils import create_example_model
from tests.fixtures import config  # noqa


def test_export_new_raw_data(dbsession, monkeypatch, config):
    tmp_path = config['ALCHEMY_FILESTORE_DIR']
    ctx = create_example_model(dbsession, tmp_path)

    model = dbsession.query(TextClassificationModel).first()

    from alchemy.bg.jobs import export_new_raw_data
    from alchemy.shared.utils import load_jsonl

    fname = "test_export_new_raw_data.jsonl"

    export_new_raw_data(model, ctx["data_fname"], fname)

    expected_output_path = str(tmp_path / "raw_data" / fname)
    assert os.path.isfile(expected_output_path)
    df = load_jsonl(expected_output_path)
    assert len(df) == 3

    with pytest.raises(Exception) as e:
        export_new_raw_data(model, ctx["data_fname"], fname)
        assert "Cannot overwrite existing file" in e
