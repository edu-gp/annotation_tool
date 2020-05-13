import time
from celery import Celery
from train.gcp_job import GCPJob
from db.model import Database, Model
from db.config import DevelopmentConfig

app = Celery(
    # module name
    'gcp_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def poll_status(model_id):
    db = Database.from_config(DevelopmentConfig)
    try:
        model = db.session.query(Model).filter_by(id=model_id).one_or_none()

        assert model, f"Model not found model_id={model_id}"

        job = GCPJob(model.uuid, model.version)

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
    finally:
        db.session.close()


app.conf.task_routes = {'*.gcp_celery.*': {'queue': 'gcp_celery'}}

'''
celery --app=train.gcp_celery worker -Q gcp_celery -l info -n gcp_celery
'''
