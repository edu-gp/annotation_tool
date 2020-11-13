import json

from alchemy.db.model import _raw_data_file_path, db
from alchemy.shared.file_adapters import load_jsonl
from tests.fixtures import admin_server_client  # noqa
from tests.utils import create_example_model, assert_file_exists


def test_export_new_raw_data(monkeypatch, admin_server_client, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    ctx = create_example_model(db.session, cloud=(data_store == 'cloud'))

    from alchemy.db.model import Model

    model = db.session.query(Model).first()
    assert model is not None

    mimetype = "application/json"
    headers = {"Content-Type": mimetype, "Accept": mimetype}
    data = {
        "model_id": "1",
        "data_fname": ctx["data_fname"],
        "output_fname": "blah.jsonl",
        "cutoff": "0.9",
    }

    response = admin_server_client.post(
        "/models/export_new_raw_data", data=json.dumps(data), headers=headers
    )

    assert response.status == "200 OK"
    assert response.get_json()["error"] is None

    output_path = _raw_data_file_path("blah.jsonl")
    assert_file_exists(output_path, local=(data_store == 'local'), cloud=(data_store == 'cloud'))

    df = load_jsonl(output_path, data_store=data_store)
    assert len(df) == 2
