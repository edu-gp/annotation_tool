import logging
import os
import time

from celery import Celery

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import Database, ModelDeploymentConfig, TextClassificationModel
from alchemy.train.gcp_celery import poll_status as gcp_poll_status
from alchemy.train.gcp_job import ModelDefn, submit_job
from alchemy.train.gs_utils import (
    DeployedInferenceMetadata,
    create_deployed_inference,
    has_model_inference,
)
from alchemy.train.prep import prepare_next_model_for_label

app = Celery(
    # module name
    "train_celery",
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
            entity_type=entity_type,
        )

        model_defn = ModelDefn(model.uuid, model.version)
        job = submit_job(model_defns=[model_defn], files_for_inference=[raw_file_path])
        gcp_poll_status.delay(job.id)
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
            model = db.session.query(TextClassificationModel).get(config.model_id)

            # Note: The deployment configs may be modified while inference is
            # running, so we want to make a copy of the config and pass it to
            # the training job.
            metadata = DeployedInferenceMetadata(
                timestamp=timestamp,
                model_uuid=model.uuid,
                model_version=model.version,
                label=model.label,
                threshold=config.threshold,
                dataset_name=dataset_name,
            )

            if has_model_inference(model.uuid, model.version, dataset_name):
                create_deployed_inference(metadata)
            else:
                # Kick off a new job to run inference, then deploy when done.
                model_defn = ModelDefn(model.uuid, model.version)
                job = submit_job(
                    model_defns=[model_defn], files_for_inference=[dataset_name]
                )
                gcp_poll_status.delay(job.id, metadata_dict=metadata.to_dict())
    finally:
        db.session.close()


app.conf.task_routes = {"*.train_celery.*": {"queue": "train_celery"}}

"""
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
"""
