import logging
import os
from celery import Celery

from alchemy.ar import generate_annotation_requests as _generate_annotation_requests
from alchemy.ar.data import save_new_ar_for_user_db
from alchemy.db.model import Database, Task, get_or_create
from alchemy.shared.celery_job_status import JobStatus, set_status

app = Celery(
    # module name
    "ar_celery",
    # redis://:password@hostname:port/db_number
    broker=f"redis://{os.getenv('REDIS_HOST', 'localhost')}:6379/0",
    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def hello():
    print("HI")


@app.task
def generate_annotation_requests(task_id, max_per_annotator, max_per_dp, entity_type):
    celery_id = str(generate_annotation_requests.request.id)
    set_status(celery_id, JobStatus.STARTED, progress=0.0)

    logging.info(
        f"Generate max={max_per_annotator} annotations per user with max_per_dp={max_per_dp}, "
        f"task_id={task_id}, entity_type={entity_type}"
    )

    db = Database.bootstrap()
    res = _generate_annotation_requests(
        db.session, task_id, max_per_annotator, max_per_dp
    )

    task = get_or_create(dbsession=db.session, model=Task, id=task_id)

    # TODO Defaulting to the first label of the task.
    label = task.get_labels()[0]

    count = 0
    for username, annotation_requests in res.items():
        logging.info("Creating annotation requests for user {}".format(username))
        #  Here we have a user and a list of request in the form of a
        #  dictionary and we want to save it for this user in db.
        save_new_ar_for_user_db(
            db.session,
            task_id,
            username,
            annotation_requests,
            label,
            entity_type,
            clean_existing=True,
        )
        count += len(annotation_requests)
    print(f"Done")
    print("The number of requests processed: {}".format(count))

    set_status(celery_id, JobStatus.DONE, progress=1.0)

    db.session.close()


app.conf.task_routes = {"*.ar_celery.*": {"queue": "ar_celery"}}

"""
celery --app=ar.ar_celery worker -Q ar_celery -c 1 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -Q ar_celery -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery
"""
