from celery import Celery
from db.model import Database
from db.config import DevelopmentConfig
from train.prep import prepare_next_model_for_label
from train.no_deps.run import (
    train_model as _train_model,
    inference as _inference
)
from train.gcp_job import ModelDefn, submit_job
from train.gcp_celery import poll_status as gcp_poll_status

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
def submit_gcp_inference(model_id, raw_file_path):
    '''
    TODO: Use this function in prod when new data arrives.

    Staging test:
    from train.train_celery import submit_gcp_inference
    submit_gcp_inference.delay(12, 'spring_jan_2020.jsonl')
    '''
    from db.model import TextClassificationModel
    db = Database.from_config(DevelopmentConfig)
    try:
        model = db.session.query(TextClassificationModel).get(model_id)
        assert model, f"Model not found - model_id={model_id}"

        model_defn = ModelDefn(model.uuid, model.version)

        job_id = submit_job(model_defns=[model_defn],
                            files_for_inference=[raw_file_path])

        gcp_poll_status.delay(job_id)
    finally:
        db.session.close()


app.conf.task_routes = {'*.train_celery.*': {'queue': 'train_celery'}}

'''
celery --app=train.train_celery worker -Q train_celery -c 1 -l info --max-tasks-per-child 1 -P threads -n train_celery
'''
