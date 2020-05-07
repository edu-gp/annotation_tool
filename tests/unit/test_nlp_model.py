from tests.sqlalchemy_conftest import *
from tests.utils import create_example_model
from db.model import Task, TextClassificationModel
from inference.nlp_model import NLPModel
import numpy as np
import json


def test_create(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    create_example_model(dbsession, tmp_path)

    # Using NLPModel

    task = dbsession.query(Task).first()

    # By passing in a task, we'll use the task's latest model's prediction.
    nlp_model = NLPModel(dbsession, task.id)

    assert nlp_model.predict(['unknown piece of text']) == \
        [{'score': 0.0, 'prob': None}]
    assert nlp_model.predict(['hello'])[0]['prob'] > 0

    assert len(nlp_model._cache) == 3
