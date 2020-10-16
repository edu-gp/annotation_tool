from envparse import env
from pathlib import Path


def build_raw_data_dir() -> str:
    bucket = env('GOOGLE_AI_PLATFORM_BUCKET', default=None)
    assert bucket, "GCS bucket not defined"  # TODO: If no default is set env will automatically raise an exception, so we might be able to remove this assertion
    return f"gs://{bucket}/data"


def build_raw_data_url(dataset_name) -> str:
    dataset_name_stem = Path(dataset_name).stem
    return f"{build_raw_data_dir()}/{dataset_name_stem}.jsonl"


def build_model_dir(model_uuid, model_version) -> str:
    bucket = env('GOOGLE_AI_PLATFORM_BUCKET', default=None)
    assert bucket, "GCS bucket not defined"  # TODO: above
    return f"gs://{bucket}/tasks/{model_uuid}/models/{model_version}"


def build_model_inference_url(model_uuid, model_version, dataset_name) -> str:
    dataset_name_stem = Path(dataset_name).stem
    model_dir = build_model_dir(model_uuid, model_version)
    return f"{model_dir}/inference/{dataset_name_stem}.pred.npy"


def _build_prod_dir(model_uuid, model_version, dataset_name, ts) -> str:
    stem = Path(dataset_name).stem
    bucket = env('GOOGLE_AI_PLATFORM_BUCKET', None)
    assert bucket, "GCS bucket not defined"  # TODO: above
    return f"gs://{bucket}/prod/{model_uuid}/{model_version}/{stem}/{ts}"


def build_prod_inference_url(model_uuid, model_version, dataset_name, ts) -> str:
    dirname = _build_prod_dir(model_uuid, model_version, dataset_name, ts)
    return dirname + "/inference.csv"


def build_prod_metadata_url(model_uuid, model_version, dataset_name, ts) -> str:
    dirname = _build_prod_dir(model_uuid, model_version, dataset_name, ts)
    return dirname + "/metadata.json"
