import os
from typing import List

import numpy as np

from alchemy.db.fs import raw_data_dir
from alchemy.db.model import (
    ClassificationAnnotation,
    EntityTypeEnum,
    Task,
    User,
    majority_vote_annotations_query,
)
from alchemy.train.no_deps.inference_results import InferenceResults
from alchemy.train.no_deps.paths import (  # Train Model; Model Inference
    _get_data_parser_fname,
    _get_inference_fname,
    _get_metrics_fname,
)
from alchemy.train.no_deps.run import build_inference_cache, inference, train_model
from alchemy.train.no_deps.utils import BINARY_CLASSIFICATION
from alchemy.train.no_deps.utils import listdir, load_json, load_jsonl, save_jsonl
from alchemy.train.prep import prepare_next_model_for_label

LABEL = "IsTall"

# TODO add a case to cover only 1 class present in the data.
# This is easy to do, just set N=2.
# Unclear what's the best way to surface that error just yet.


class stub_model:
    def __init__(self):
        self.history = []

    def predict(self, text: List[str]):
        self.history.append(text)
        preds = np.array([1] * len(text))
        # This is the style when "Sliding Window" is enabled
        probs = [np.array([[-0.48803285, 0.56392884]], dtype=np.float32)] * len(text)
        return preds, probs


def stub_train_fn(X, y, config):
    return stub_model()


def stub_build_fn(config, model_dir):
    return stub_model()


# Create many Annotations for a Label
def _create_anno(ent, v, user, weight=1):
    return ClassificationAnnotation(
        entity_type=EntityTypeEnum.COMPANY,
        entity=ent,
        user=user,
        label=LABEL,
        value=v,
        weight=weight,
    )


def _populate_db_and_fs(dbsession, tmp_path, N, data_store, weight=1):
    # =========================================================================
    # Add in a fake data file
    d = raw_data_dir(tmp_path, as_path=True)
    p = d / "data.jsonl"
    data = [
        {"text": f"item {i} text", "meta": {"domain": f"{i}.com"}} for i in range(N)
    ]
    save_jsonl(str(p), data, data_store=data_store)

    # =========================================================================
    # Create dummy data

    # Create many Users
    user = User(username=f"someuser")
    dbsession.add(user)

    dbsession.commit()

    ents = [d["meta"]["domain"] for d in data]
    annos = [
        # Create a few annotations for the first 2 entities.
        _create_anno(ents[0], 1, user),
        _create_anno(ents[0], 1, user),
        _create_anno(ents[0], 1, user),
        _create_anno(ents[0], -1, user, weight=weight),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[1], 1, user, weight=weight),
        _create_anno(ents[1], 1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], 0, user),
        _create_anno(ents[1], 0, user),
    ]
    for i in range(2, N):
        # Create one annotations for the rest of the entities.
        annos.append(_create_anno(ents[i], 1 if i % 2 else -1, user))

    dbsession.add_all(annos)
    dbsession.commit()

    # Create a Task
    task = Task(name="Bball")
    task.set_labels([LABEL])
    task.set_annotators([user.username])
    task.set_patterns_file(None)
    task.set_patterns(["Shaq", "Lebron"])
    task.set_data_filenames(["data.jsonl"])
    dbsession.add(task)
    dbsession.commit()


def test_train_flow_simple(dbsession, monkeypatch, tmp_path):
    data_store = 'local'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))
    N = 2
    _populate_db_and_fs(dbsession, tmp_path, N, data_store=data_store, weight=100)
    query = majority_vote_annotations_query(dbsession, LABEL)
    res = query.all()
    assert sorted(res) == [("0.com", -1, 100), ("1.com", 1, 101)]


def test_train_flow_simple_equal_weight(dbsession, monkeypatch, tmp_path):
    data_store = 'local'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))
    N = 2
    _populate_db_and_fs(dbsession, tmp_path, N, data_store=data_store, weight=3)
    query = majority_vote_annotations_query(dbsession, LABEL)
    res = query.all()
    assert len(res) == N
    res = sorted(res)
    for i in range(N):
        assert res[i][0] == str(i) + ".com"
        assert res[i][1] in [-1, 1]
        if i == 0:
            assert res[i][2] == 3
        elif i == 1:
            assert res[i][2] == 4


def test_train_flow(dbsession, monkeypatch, tmp_path):
    data_store = 'local'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))
    N = 20
    _populate_db_and_fs(dbsession, tmp_path, N, data_store=data_store)
    task = dbsession.query(Task).first()

    # Part 1. Prepare.
    label = task.get_labels()[0]
    raw_file_path = task.get_data_filenames(abs=True)[0]
    model = prepare_next_model_for_label(dbsession, label, raw_file_path, data_store=data_store)
    model_dir = model.dir

    # These are all the files we need to train a model.
    assert set(listdir(model_dir, data_store=data_store)) == {"data.jsonl", "config.json"}

    data = load_jsonl(os.path.join(model_dir, "data.jsonl"), to_df=False, data_store=data_store)
    data = sorted(data, key=lambda d: d["text"])
    print(data[0])
    print(data[1])
    print(len(data))
    assert data[0] == {"text": "item 0 text", "labels": {"IsTall": 1}}
    assert data[1] == {"text": "item 1 text", "labels": {"IsTall": -1}}
    assert len(data) == 20

    config = load_json(os.path.join(model_dir, "config.json"), data_store=data_store)
    assert config is not None
    assert config["train_config"] is not None

    # Part 2. Train model.
    train_model(model_dir, train_fn=stub_train_fn)

    data_parser_results = load_json(_get_data_parser_fname(model_dir), data_store=data_store)
    assert data_parser_results["problem_type"] == BINARY_CLASSIFICATION

    metrics = load_json(_get_metrics_fname(model_dir), data_store=data_store)
    assert metrics["test"] is not None
    assert metrics["train"] is not None

    # Part 3. Post-training Inference.
    f = tmp_path / "tmp_file_for_inference.jsonl"
    save_jsonl(str(f), [{"text": "hello"}, {"text": "world"}], data_store=data_store)

    inference(model_dir, str(f), build_model_fn=stub_build_fn, generate_plots=False, data_store=data_store)

    ir = InferenceResults.load(_get_inference_fname(model_dir, str(f)), data_store=data_store)
    assert np.isclose(ir.probs, [0.7411514, 0.7411514]).all()

    # Part 4. New data update, run inference on the new data.
    f2 = tmp_path / "tmp_file_for_inference_v2.jsonl"
    save_jsonl(
        str(f2),
        [
            {"text": "hello"},
            {"text": "world"},
            {"text": "newline_1"},
            {"text": "newline_2"},
        ],
        data_store=data_store
    )

    class Mock_DatasetStorageManager:
        def download(self, url):
            # Return the one data file we have locally.
            return str(f)

    mock_dsm = Mock_DatasetStorageManager()

    inference_cache = build_inference_cache(model_dir, dsm=mock_dsm, data_store=data_store)
    model, _ = inference(
        model_dir,
        str(f2),
        build_model_fn=stub_build_fn,
        generate_plots=False,
        inference_cache=inference_cache,
        data_store=data_store
    )

    assert model.history == [
        ["newline_1", "newline_2"]
    ], "Model should have only been ran on the new lines"

    ir2 = InferenceResults.load(_get_inference_fname(model_dir, str(f2)), data_store=data_store)
    assert len(ir2.probs) == 4, "Inference should have 4 elements"
    assert ir.probs[:2] == ir2.probs[:2], "Result on the same items should be the same"


def test_majority_vote_annotations_query(dbsession):
    user = User(username="fake_user")
    dbsession.add(user)

    dbsession.commit()

    ents = [str(i) + ".com" for i in range(3)]
    annos = [
        # for this entity, the value -1 has the highest weight.
        _create_anno(ents[0], 1, user),
        _create_anno(ents[0], 1, user),
        _create_anno(ents[0], 1, user),
        _create_anno(ents[0], -1, user, weight=100),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[0], 0, user),
        _create_anno(ents[0], 0, user),
        # for this entity, the weighted vote is a tie between 1 and -1.
        _create_anno(ents[1], 1, user, weight=3),
        _create_anno(ents[1], 1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], -1, user),
        _create_anno(ents[1], 0, user),
        _create_anno(ents[1], 0, user),
        _create_anno(ents[2], 1, user),
    ]

    dbsession.add_all(annos)
    dbsession.commit()

    query = majority_vote_annotations_query(dbsession, label=LABEL)
    res = query.all()
    assert len(res) == 3
    res = sorted(res, key=lambda x: x[0])
    for i in range(3):
        if i == 0:
            assert res[0] == ("0.com", -1, 100)
        elif i == 1:
            assert res[1][0] == "1.com"
            assert res[1][1] in [1, -1]
            assert res[1][2] == 4
        else:
            assert res[2] == ("2.com", 1, 1)
