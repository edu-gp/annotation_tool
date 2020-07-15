import time
from typing import List, Optional
from celery import Celery
from db.model import (
    Database, Model, TextClassificationModel, ModelDeploymentConfig
)
from db.config import DevelopmentConfig
from train.prep import prepare_next_model_for_label
from train.no_deps.run import (
    train_model as _train_model,
    inference as _inference
)
from train.gcp_job import ModelDefn, submit_job
from train.gcp_celery import poll_status as gcp_poll_status
from train.gs_utils import (
    has_model_inference, create_deployed_inference, DeployedInferenceMetadata
)

app = Celery(
    # module name
    'train_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)

# NOTE:
# - Celery doesn't allow tasks to spin up other processes - I have to run it in Threads mode
# - When a model is training, even cold shutdown doesn't work


@app.task
def train_model(label, raw_file_path, entity_type):
    db = Database.from_config(DevelopmentConfig)
    try:
        model = prepare_next_model_for_label(
            db.session,
            label=label,
            raw_file_path=raw_file_path,
            entity_type=entity_type
        )
        model_dir = model.dir(abs=True)

        _train_model(model_dir)

        # Note: It appears inference can be faster if it's allowed to use all the GPU memory,
        # however the only way to clear all GPU memory is to end this task. So we call inference
        # asynchronously so this task can end.
        inference.delay(model_dir, raw_file_path)
    finally:
        db.session.close()


@app.task
def inference(model_dir, raw_file_path):
    _inference(model_dir, [raw_file_path])


@app.task
def submit_gcp_training(label, raw_file_path, entity_type):
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


# TODO deprecate; this is not used anywhere except when debugging.
@app.task
def submit_gcp_inference(label, version, raw_file_path):
    '''
    TODO: Use this function in prod when new data arrives.

    Staging test:
    from train.train_celery import submit_gcp_inference
    submit_gcp_inference.delay('Healthcare', 7, 'spring_jan_2020.jsonl')
    '''
    db = Database.from_config(DevelopmentConfig)
    try:
        model = db.session.query(TextClassificationModel).filter_by(
            label=label, version=version).one_or_none()

        assert model, f"Model not found - label={label} version={version}"

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
    for dataset_name in files_for_inference:
        ensure_file_exists_locally(dataset_name)

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
