import os
from pathlib import Path

from alchemy.shared.file_adapters import save_jsonl
from alchemy.train.gs_utils import (
    _get_topic_name_on_stage,
    build_prod_inference_dataframe,
)
from alchemy.train.no_deps.inference_results import InferenceResults


def test_build_prod_inference_dataframe(monkeypatch, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = Path('__filestore')
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    pred_path = str(tmp_path / "valid.npy")
    InferenceResults([[20, 20], [-10, 20]], data_store=data_store).save(pred_path)

    raw_path = str(tmp_path / "raw.jsonl")
    data = [
        {"text": "hello world", "meta": {"domain": "a.com", "name": "Entity A"}},
        {"text": "goodbye world", "meta": {"domain": "z.com", "name": "Entity Z"}},
    ]
    save_jsonl(raw_path, data, data_store=data_store)

    df = build_prod_inference_dataframe(pred_path, raw_path, 0.9, data_store=data_store)

    assert df.iloc[0]["text"] == "hello world"
    assert df.iloc[0]["meta_domain"] == "a.com"
    assert df.iloc[0]["meta_name"] == "Entity A"
    assert abs(df.iloc[0]["prob"] - 0.5) < 0.0001
    assert df.iloc[0]["pred"] == False

    assert df.iloc[1]["text"] == "goodbye world"
    assert df.iloc[1]["meta_domain"] == "z.com"
    assert df.iloc[1]["meta_name"] == "Entity Z"
    assert abs(df.iloc[1]["prob"] - 1.0) < 0.0001
    assert df.iloc[1]["pred"] == True


def test___get_topic_name_on_stage(monkeypatch):
    monkeypatch.setenv("ENV_STAGE", "dev")
    monkeypatch.setenv("INFERENCE_OUTPUT_PUBSUB_TOPIC_DEV", "dev_topic")

    assert _get_topic_name_on_stage(os.getenv("ENV_STAGE")) == "dev_topic"

    monkeypatch.setenv("ENV_STAGE", "beta")
    monkeypatch.setenv("INFERENCE_OUTPUT_PUBSUB_TOPIC_BETA", "beta_topic")

    assert _get_topic_name_on_stage(os.getenv("ENV_STAGE")) == "beta_topic"

    monkeypatch.setenv("ENV_STAGE", "prod")
    monkeypatch.setenv("INFERENCE_OUTPUT_PUBSUB_TOPIC_PROD", "prod_topic")

    assert _get_topic_name_on_stage(os.getenv("ENV_STAGE")) == "prod_topic"
