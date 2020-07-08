from celery import Celery
from db.utils import get_all_data_files
from db.model import Database, TextClassificationModel
from db.config import DevelopmentConfig
from train.prep import prepare_next_model_for_label
from train.no_deps.run import (
    train_model as _train_model,
    inference as _inference
)
from train.gcp_job import ModelDefn, submit_job
from train.gcp_celery import poll_status as gcp_poll_status
from train.bg_utils import (
    download_file, get_selected_deployment_configs, already_has_inference,
    build_results_for_production, DeployedInferenceMetadata
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

        model_defn = ModelDefn(model.uuid, model.version)

        job_id = submit_job(model_defns=[model_defn],
                            files_for_inference=[raw_file_path])

        gcp_poll_status.delay(job_id)
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

        model_defn = ModelDefn(model.uuid, model.version)

        job_id = submit_job(model_defns=[model_defn],
                            files_for_inference=[raw_file_path])

        gcp_poll_status.delay(job_id)
    finally:
        db.session.close()


@app.task
def submit_gcp_inference_on_new_file(filename):
    # TODO Tests

    # Ensure the file exists
    if filename not in get_all_data_files():
        download_file(filename)

    if filename not in get_all_data_files():
        raise Exception(f"File {filename} either does not exist or is invalid")

    # Check which models need to be ran, and kick them off.
    db = Database.from_config(DevelopmentConfig)
    try:
        # TODO get config, model uuid and version in 1 SQL call?
        configs = get_selected_deployment_configs(db.session)

        for config in configs:
            model = db.session.query(
                TextClassificationModel).get(config.model_id)

            # Note: The deployment configs may be modified while inference is
            # running, so we want to make a copy of the config and pass it to
            # the training job.
            metadata = DeployedInferenceMetadata(
                model_uuid=model.uuid,
                model_version=model.version,
                threshold=config.threshold,
                filename=filename)

            if already_has_inference(model, filename):
                build_results_for_production(metadata)
            else:
                # Kick off a new job to run inference, when it's done, build
                # the results for production.
                model_defn = ModelDefn(model.uuid, model.version)
                job_id = submit_job(model_defns=[model_defn],
                                    files_for_inference=[filename])

                gcp_poll_status.delay(job_id, metadata_dict=metadata.to_dict())
    finally:
        db.session.close()


app.conf.task_routes = {'*.train_celery.*': {'queue': 'train_celery'}}

'''
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
'''
