from celery import Celery

from ar import generate_annotation_requests as _generate_annotation_requests
from ar.data import save_new_ar_for_user

from shared.celery_job_status import CeleryJobStatus

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
def generate_annotation_requests(task_id, n, overlap):
    celery_id = str(generate_annotation_requests.request.id)

    # cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    # cjs.set_state_started()
    # import time
    # for i in range(30):
    #     cjs.set_progress(i/30)
    #     time.sleep(1)
    # cjs.set_progress(1)
    # cjs.set_state_done()

    cjs = CeleryJobStatus.fetch_by_celery_id(celery_id)
    cjs.set_state_started()

    print(f"Generate max={n} annotations per user with max overlap={overlap}, task_id={task_id}")
    res = _generate_annotation_requests(task_id, n, overlap)
    for user_id, annotation_requests in res.items():
        save_new_ar_for_user(task_id, user_id, annotation_requests, clean_existing=True)
    print(f"Done")

    cjs.set_progress(1)
    cjs.set_state_done()

app.conf.task_routes = {'*.ar_celery.*': {'queue': 'ar_celery'}}

'''
celery --app=ar.ar_celery worker -Q ar_celery -c 1 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -Q ar_celery -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery
'''
