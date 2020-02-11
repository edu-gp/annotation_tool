from celery import Celery

from ar import generate_annotation_requests_for_user as _generate_annotation_requests_for_user

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
def generate_annotation_requests_for_user(task_id, user_id, n):
    print(f"Generate {n} tasks for user {user_id}, task_id={task_id}")
    fname = _generate_annotation_requests_for_user(task_id, user_id, n)
    print(f"Done: {fname}")

app.conf.task_routes = {'*.ar_celery.*': {'queue': 'ar_celery'}}

'''
celery --app=ar.ar_celery worker -Q ar_celery -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery

celery --app=ar.ar_celery worker -c 2 --autoscale=100,2 -l info --max-tasks-per-child 1 -n ar_celery
'''
