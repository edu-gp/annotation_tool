import time
from celery import Celery
from train.prep import get_next_version, prepare_task_for_training
from train.gcp_job import GCPJob

app = Celery(
    # module name
    'gcp_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def train_model(task_id):
    version = get_next_version(task_id)
    prepare_task_for_training(task_id, version)

    job = GCPJob(task_id, version)

    # TODO: A duplicate job would error out.
    job.submit()

    while True:
        status = job.get_status()

        if status is None:
            print("Unknown job status")
            break
            # TODO: Print the actual error.
        else:
            print(status)

            if status.get('state') == 'SUCCEEDED':
                job.download()
                break

        time.sleep(60)


app.conf.task_routes = {'*.gcp_celery.*': {'queue': 'gcp_celery'}}

'''
celery --app=train.gcp_celery worker -Q gcp_celery -l info -n gcp_celery
'''
