import logging
import os
import time
from typing import List, Optional
from celery import Celery
from db.model import (
    Database, Model, TextClassificationModel, ModelDeploymentConfig
)
from db.config import DevelopmentConfig
from train.prep import prepare_next_model_for_label
from train.gcp_job import ModelDefn, submit_job
from train.gcp_celery import poll_status as gcp_poll_status
from train.gs_utils import (
    has_model_inference, create_deployed_inference, DeployedInferenceMetadata
)
from pathlib import Path

app = Celery(
    # module name
    'train_celery',

    # redis://:password@hostname:port/db_number
    broker=f"redis://{os.getenv('REDIS_HOST', 'localhost')}:6379/0",

    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def submit_gcp_training(label, raw_file_path, entity_type):
    logging.info("Raw file for the training is " + raw_file_path)
    db = Database.from_config(DevelopmentConfig)
    try:
        model = prepare_next_model_for_label(
            db.session,
            label=label,
            raw_file_path=raw_file_path,
            entity_type=entity_type
        )

        submit_gcp_job(model, [raw_file_path])
    finally:
        db.session.close()


@app.task
def submit_gcp_inference_on_new_file(dataset_name):
    # TODO test

    # Check which models need to be ran, and kick them off.
    timestamp = int(time.time())
    db = Database.from_config(DevelopmentConfig)
    try:
        configs = ModelDeploymentConfig.get_selected_for_deployment(db.session)

        for config in configs:
            model = db.session.query(
                TextClassificationModel).get(config.model_id)

            # Note: The deployment configs may be modified while inference is
            # running, so we want to make a copy of the config and pass it to
            # the training job.
            metadata = DeployedInferenceMetadata(
                timestamp=timestamp,
                model_uuid=model.uuid,
                model_version=model.version,
                label=model.label,
                threshold=config.threshold,
                dataset_name=dataset_name)

            if has_model_inference(model.uuid, model.version, dataset_name):
                create_deployed_inference(metadata)
            else:
                # Kick off a new job to run inference, then deploy when done.
                submit_gcp_job(model, [dataset_name], deploy_metadata=metadata)
    finally:
        db.session.close()


def submit_gcp_job(model: Model, files_for_inference: List[str],
                   deploy_metadata: Optional[DeployedInferenceMetadata] = None):
    """Submits a training & inference job onto Google AI Platform.

    Inputs:
        model: -
        metadata: If a metadata is not None, it means we intend for the
            results to be deployed.
    """
    for filepath in files_for_inference:
        filename = Path(filepath).name
        ensure_file_exists_locally(filename)

    model_defn = ModelDefn(model.uuid, model.version)

    job_id = submit_job(model_defns=[model_defn],
                        files_for_inference=files_for_inference)

    if deploy_metadata:
        gcp_poll_status.delay(job_id, metadata_dict=deploy_metadata.to_dict())
    else:
        gcp_poll_status.delay(job_id)


def ensure_file_exists_locally(dataset_name: str) -> None:
    """Ensure data `dataset_name` from GCS is available locally.
    Inputs:
        dataset_name: A data dataset_name e.g. "jan_2020.jsonl"
    Raises:
        Exception if the file could not be present locally.
    """
    # TODO test
    # TODO refactor to another file (but where?)
    from db.utils import get_all_data_files, get_local_data_file_path
    from train.gs_url import build_raw_data_url
    from train.no_deps.utils import gs_copy_file

    # Ensure the file exists locally
    if dataset_name not in get_all_data_files():
        remote_fname = build_raw_data_url(dataset_name)
        local_fname = get_local_data_file_path(dataset_name)
        gs_copy_file(remote_fname, local_fname)

    if dataset_name not in get_all_data_files():
        raise Exception(
            f"Dataset {dataset_name} either does not exist or is invalid")


app.conf.task_routes = {'*.train_celery.*': {'queue': 'train_celery'}}

'''
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
'''
