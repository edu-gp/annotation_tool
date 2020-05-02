from tests.sqlalchemy_conftest import *
from db.model import Task, TextClassificationModel
from inference.nlp_model import NLPModel
from shared.utils import stem
import numpy as np
from numpy import save
import json


def test_create(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    # =========================================================================
    # Create mock files

    model_uuid = "abc"
    version = 1
    data_fname = "myfile.jsonl"

    # Save the data file
    d = tmp_path / "raw_data"
    d.mkdir(parents=True)

    p = d / data_fname

    _raw_text = [
        {"text": "hello", "meta": {"domain": "a.com", "name": "a"}},
        {"text": "bonjour", "meta": {"domain": "b.com", "name": "b"}},
        {"text": "nihao", "meta": {"domain": "c.com", "name": "c"}},
    ]
    p.write_text("\n".join([json.dumps(t) for t in _raw_text]))

    # Save the predictions
    d = tmp_path / "models" / model_uuid / str(version) / "inference"
    d.mkdir(parents=True)

    p = d / f"{stem(data_fname)}.pred.npy"
    raw_results = np.array([
        [0.1234, 0.234],
        [-2.344, 0.100],
        [-2.344, 0.100],
    ])
    save(p, raw_results)

    # A Task has a Model.
    task = Task(name="mytask", default_params={
        'data_filenames': [data_fname]
    })
    model = TextClassificationModel(
        uuid=model_uuid, version=version, task=task)

    dbsession.add_all([task, model])
    dbsession.commit()

    # =========================================================================
    # Using NLPModel

    task = dbsession.query(Task).first()

    # By passing in a task, we'll use the task's latest model's prediction.
    nlp_model = NLPModel(dbsession, task.id)

    assert nlp_model.predict(['unknown piece of text']) == \
        [{'score': 0.0, 'prob': None}]
    assert nlp_model.predict(['hello'])[0]['prob'] > 0

    assert len(nlp_model._cache) == 3
