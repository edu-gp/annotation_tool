import logging

from celery import Celery

from ar import generate_annotation_requests as _generate_annotation_requests
from ar.data import save_new_ar_for_user_db
from db.config import DevelopmentConfig
from db.model import db, Database

from shared.celery_job_status import set_status, JobStatus

app = Celery(
    # module name
    'ar_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def hello():
    print("HI")


@app.task
def generate_annotation_requests(task_id, max_per_annotator,
                                 max_per_dp):
    logging.error("Here here")
    print("Here here")
    celery_id = str(generate_annotation_requests.request.id)
    set_status(celery_id, JobStatus.STARTED, progress=0.0)

    logging.error(
        f"Generate max={max_per_annotator} annotations per user with max_per_dp={max_per_dp}, task_id={task_id}")
    # TODO Touching file systems, need to migrate
    db = Database.from_config(DevelopmentConfig)
    res = _generate_annotation_requests(
        db.session,
        task_id,
        max_per_annotator,
        max_per_dp)
    for user_id, annotation_requests in res.items():
        logging.error("Creating annotation requests for user {}".
                      format(user_id))
        # TODO touching file system, need to migrate
        #  So here we have a user and a list of request in the form of a
        #  dictionary and we want to save it for this user in db.
        save_new_ar_for_user_db(
            db.session, task_id, user_id, annotation_requests,
            clean_existing=True)
    print(f"Done")

    set_status(celery_id, JobStatus.DONE, progress=1.0)


app.conf.task_routes = {'*.ar_celery.*': {'queue': 'ar_celery'}}

'''
celery --app=ar.ar_celery worker -Q ar_celery -c 1 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -Q ar_celery -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery
'''
