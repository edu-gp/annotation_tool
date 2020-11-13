import json
import os
from pathlib import Path

import numpy as np
from envparse import env
from google.cloud import storage
from numpy import save

from alchemy.db.fs import bucket
from alchemy.db.model import Task, TextClassificationModel
from alchemy.shared.file_adapters import file_exists, save_json
from alchemy.shared.utils import stem


def fake_train_model(model, filestore_base_dir, data_store):
    metrics_fname = os.path.join(
        filestore_base_dir, "models", model.uuid, str(model.version), "metrics.json"
    )
    save_json(metrics_fname, data={"accuracy": 0.95}, data_store=data_store)


def create_example_model(dbsession, cloud):
    root_dir = env('ALCHEMY_FILESTORE_DIR', cast=Path)

    # Create mock files
    model_uuid = "abc"
    version = 1
    data_fname = "myfile.jsonl"

    # Save the data file
    d = root_dir / "raw_data"
    d.mkdir(parents=True, exist_ok=True)

    p = d / data_fname

    _raw_text = [
        {"text": "hello", "meta": {"domain": "a.com", "name": "a"}},
        {"text": "bonjour", "meta": {"domain": "b.com", "name": "b"}},
        {"text": "nihao", "meta": {"domain": "c.com", "name": "c"}},
    ]
    _raw_text_str = "\n".join([json.dumps(t) for t in _raw_text])
    if cloud:
        blob = storage.Blob(str(p), bucket())
        blob.upload_from_string(_raw_text_str)
    else:
        p.write_text(_raw_text_str)

    # Save the predictions
    d = root_dir / "models" / model_uuid / str(version) / "inference"
    d.mkdir(parents=True, exist_ok=True)

    p = d / f"{stem(data_fname)}.pred.npy"
    raw_results = np.array(
        [
            [0.1234, 0.234],  # prob = 0.527
            [-2.344, 0.100],  # prob = 0.920
            [-2.344, 0.100],  # prob = 0.920
        ]
    )
    save(p, raw_results)
    if cloud:
        blob = storage.Blob(str(p), bucket())
        blob.upload_from_filename(p)

    # A Task has a Model.
    task = Task(name="mytask", default_params={"data_filenames": [data_fname]})
    model = TextClassificationModel(uuid=model_uuid, version=version)

    dbsession.add_all([task, model])
    dbsession.commit()

    return {"data_fname": data_fname}


def assert_file_exists(filename, local=True, cloud=False):
    if local:
        assert file_exists(filename, data_store='local')
    if cloud:
        assert file_exists(filename, data_store='cloud')
