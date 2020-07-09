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
    ensure_file_exists_locally, model_has_inference, create_deployed_inference,
    DeployedInferenceMetadata
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


@app.task
def submit_gcp_inference(label, version, raw_file_path):
    '''
    TODO: Use this function in prod when new data arrives.

    Staging test:
    from train.train_celery import submit_gcp_inference
    submit_gcp_inference.delay('Healthcare', 7, 'spring_jan_2020.jsonl')
    '''
    from db.model import TextClassificationModel
    db = Database.from_config(DevelopmentConfig)
    try:
        model = db.session.query(TextClassificationModel).filter_by(
            label=label, version=version).one_or_none()

        assert model, f"Model not found - label={label} version={version}"

        submit_gcp_job(model, [raw_file_path])
    finally:
        db.session.close()


@app.task
def submit_gcp_inference_on_new_file(filename):
    # TODO test

    # TODO I think we only need this if we're training a new model.
    ensure_file_exists_locally(filename)

    # Check which models need to be ran, and kick them off.
    timestamp = int(time.time())
    db = Database.from_config(DevelopmentConfig)
    try:
        configs = ModelDeploymentConfig.get_selected_deployment(db.session)

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
                filename=filename)

            if model_has_inference(model.uuid, model.version, filename):
                create_deployed_inference(metadata)
            else:
                # TODO could this mean we have a model already??
                # Kick off a new job to run inference, then deploy when done.
                submit_gcp_job(model, [filename], metadata)
    finally:
        db.session.close()


def submit_gcp_job(model: Model, files_for_inference: List[str],
                   metadata: Optional[DeployedInferenceMetadata]):
    """Submits a training & inference job onto Google AI Platform.

    Inputs:
        model: -
        metadata: If a metadata is not None, it means we intend for the
            results to be deployed.
    """
    model_defn = ModelDefn(model.uuid, model.version)

    job_id = submit_job(model_defns=[model_defn],
                        files_for_inference=files_for_inference)

    if metadata:
        gcp_poll_status.delay(job_id, metadata_dict=metadata.to_dict())
    else:
        gcp_poll_status.delay(job_id)


app.conf.task_routes = {'*.train_celery.*': {'queue': 'train_celery'}}

'''
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
'''
