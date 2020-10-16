import os
import json
from envparse import env
import numpy as np
from pathlib import Path
from numpy import save
from alchemy.shared.utils import stem
from alchemy.db.model import Task, TextClassificationModel


def fake_train_model(model, filestore_base_dir):
    metrics_fname = os.path.join(filestore_base_dir, 'models', model.uuid,
                                 str(model.version), 'metrics.json')
    os.makedirs(os.path.dirname(metrics_fname), exist_ok=True)
    with open(metrics_fname, 'w') as f:
        f.write(json.dumps({'accuracy': 0.95}))


def create_example_model(dbsession):
    root_dir = env('ALCHEMY_FILESTORE_DIR', cast=Path)

    # Create mock files
    model_uuid = "abc"
    version = 1
    data_fname = "myfile.jsonl"

    # Save the data file
    d = root_dir / "raw_data"
    d.mkdir(parents=True)

    p = d / data_fname

    _raw_text = [
        {"text": "hello", "meta": {"domain": "a.com", "name": "a"}},
        {"text": "bonjour", "meta": {"domain": "b.com", "name": "b"}},
        {"text": "nihao", "meta": {"domain": "c.com", "name": "c"}},
    ]
    p.write_text("\n".join([json.dumps(t) for t in _raw_text]))

    # Save the predictions
    d = root_dir / "models" / model_uuid / str(version) / "inference"
    d.mkdir(parents=True)

    p = d / f"{stem(data_fname)}.pred.npy"
    raw_results = np.array([
        [0.1234, 0.234],  # prob = 0.527
        [-2.344, 0.100],  # prob = 0.920
        [-2.344, 0.100],  # prob = 0.920
    ])
    save(p, raw_results)

    # A Task has a Model.
    task = Task(name="mytask", default_params={
        'data_filenames': [data_fname]
    })
    model = TextClassificationModel(
        uuid=model_uuid, version=version)

    dbsession.add_all([task, model])
    dbsession.commit()

    return {
        'data_fname': data_fname
    }
