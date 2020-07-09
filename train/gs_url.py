import os
from pathlib import Path


def build_raw_data_url(filename) -> str:
    filename_stem = Path(filename).stem
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "GCS bucket not defined"
    return f"gs://{bucket}/data/{filename_stem}.jsonl"


def build_model_inference_url(model_uuid, model_version, filename) -> str:
    filename_stem = Path(filename).stem
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "GCS bucket not defined"
    return f"gs://{bucket}/tasks/{model_uuid}/models/{model_version}/inference/{filename_stem}.pred.npy"


def _build_prod_dir(model_uuid, model_version, filename, ts) -> str:
    stem = Path(filename).stem
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "GCS bucket not defined"
    return f"gs://{bucket}/prod/{model_uuid}/{model_version}/{stem}/{ts}"


def build_prod_inference_url(model_uuid, model_version, filename, ts) -> str:
    dirname = _build_prod_dir(model_uuid, model_version, filename, ts)
    return dirname + '/inference.csv'


def build_prod_metadata_url(model_uuid, model_version, filename, ts) -> str:
    dirname = _build_prod_dir(model_uuid, model_version, filename, ts)
    return dirname + '/metadata.json'
