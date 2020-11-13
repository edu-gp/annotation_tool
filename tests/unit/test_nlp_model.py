from alchemy.db.model import Model
from alchemy.inference.nlp_model import (
    NLPModel,
    NLPModelBottomResults,
    NLPModelTopResults,
)
from tests.utils import create_example_model


def test_highest_entropy_nlp_model(dbsession, monkeypatch, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    create_example_model(dbsession, cloud=(data_store == 'cloud'))

    model = dbsession.query(Model).first()

    nlp_model = NLPModel(dbsession, model.id, data_store=data_store)

    assert nlp_model.predict(["unknown piece of text"]) == [
        {"score": 0.0, "prob": None}
    ]

    assert nlp_model.predict(["hello", "bonjour", "nihao"]) == [
        {"score": 0.33734745550768536, "prob": 0.5276218490386013},
        {"score": 0.07659863544127907, "prob": 0.9201215737671476},
        {"score": 0.07659863544127907, "prob": 0.9201215737671476},
    ]

    assert len(nlp_model._cache) == 3


def test_top_bottom_nlp_model(dbsession, monkeypatch, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    create_example_model(dbsession, cloud=(data_store == 'cloud'))

    model = dbsession.query(Model).first()

    nlp_model_top = NLPModelTopResults(dbsession, model.id, data_store=data_store)

    assert nlp_model_top.predict(["hello", "bonjour", "nihao"]) == [
        {"score": 0.5276218490386013, "prob": 0.5276218490386013},
        {"score": 0.9201215737671476, "prob": 0.9201215737671476},
        {"score": 0.9201215737671476, "prob": 0.9201215737671476},
    ]

    nlp_model_bottom = NLPModelBottomResults(dbsession, model.id, data_store=data_store)

    assert nlp_model_bottom.predict(["hello", "bonjour", "nihao"]) == [
        {"score": -0.5276218490386013, "prob": 0.5276218490386013},
        {"score": -0.9201215737671476, "prob": 0.9201215737671476},
        {"score": -0.9201215737671476, "prob": 0.9201215737671476},
    ]
