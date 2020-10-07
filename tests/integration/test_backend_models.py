from tests.fixtures import *
import os
import json
from db.model import db, _raw_data_file_path
from tests.utils import create_example_model
from shared.utils import load_jsonl


def test_export_new_raw_data(admin_server_client):
    ctx = create_example_model(db.session)

    from db.model import Model
    model = db.session.query(Model).first()
    assert model is not None

    mimetype = 'application/json'
    headers = {
        'Content-Type': mimetype,
        'Accept': mimetype
    }
    data = {
        'model_id': '1',
        'data_fname': ctx['data_fname'],
        'output_fname': 'blah.jsonl',
        'cutoff': '0.9'
    }

    response = admin_server_client.post(
        '/models/export_new_raw_data',
        data=json.dumps(data),
        headers=headers
    )

    assert response.status == '200 OK'
    assert response.get_json()['error'] is None

    output_path = _raw_data_file_path('blah.jsonl')
    assert os.path.isfile(output_path)

    df = load_jsonl(output_path)
    assert len(df) == 2
