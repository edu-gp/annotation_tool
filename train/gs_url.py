import os
from pathlib import Path


def build_raw_data_url(dataset_name) -> str:
    dataset_name_stem = Path(dataset_name).stem
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "GCS bucket not defined"
    return f"gs://{bucket}/data/{dataset_name_stem}.jsonl"


def build_model_inference_url(model_uuid, model_version, dataset_name) -> str:
    dataset_name_stem = Path(dataset_name).stem
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "GCS bucket not defined"
    return f"gs://{bucket}/tasks/{model_uuid}/models/{model_version}/inference/{dataset_name_stem}.pred.npy"


def _build_prod_dir(model_uuid, model_version, dataset_name, ts) -> str:
    stem = Path(dataset_name).stem
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "GCS bucket not defined"
    return f"gs://{bucket}/prod/{model_uuid}/{model_version}/{stem}/{ts}"


def build_prod_inference_url(model_uuid, model_version, dataset_name, ts) -> str:
    dirname = _build_prod_dir(model_uuid, model_version, dataset_name, ts)
    return dirname + '/inference.csv'


def build_prod_metadata_url(model_uuid, model_version, dataset_name, ts) -> str:
    dirname = _build_prod_dir(model_uuid, model_version, dataset_name, ts)
    return dirname + '/metadata.json'
