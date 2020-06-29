import time
from celery import Celery
from train.gcp_job import GoogleAIPlatformJob, download

app = Celery(
    # module name
    'gcp_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def poll_status(job_id):
    # TODO put this on a queue, rather than just a loop check.
    while True:
        job = GoogleAIPlatformJob.fetch(job_id)

        if job is None:
            raise Exception("Unknown job")
        else:
            if job.get_state() == 'SUCCEEDED':
                for md in job.get_model_defns():
                    download(md)
                break

        time.sleep(60)


app.conf.task_routes = {'*.gcp_celery.*': {'queue': 'gcp_celery'}}

'''
celery --app=train.gcp_celery worker -Q gcp_celery -l info -n gcp_celery
'''
