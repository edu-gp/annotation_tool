import time
from typing import Optional
from celery import Celery
from train.gcp_job import GoogleAIPlatformJob, download
from train.gs_utils import create_deployed_inference, DeployedInferenceMetadata

app = Celery(
    # module name
    'gcp_celery',

    # redis://:password@hostname:port/db_number
    broker='redis://localhost:6379/0',

    # # store the results here
    # backend='redis://localhost:6379/0',
)


@app.task
def poll_status(job_id, metadata_dict: Optional[dict] = None):
    """
    Inputs:
        job_id: GoogleAIPlatformJob job id
        metadata: Production metadata. If None, then this run shouldn't go into
            the production bucket.
    """
    # TODO put this on a queue, rather than just a loop check.
    while True:
        job = GoogleAIPlatformJob.fetch(job_id)

        if job is None:
            raise Exception("Unknown job")
        else:
            if job.get_state() == 'SUCCEEDED':
                for md in job.get_model_defns():
                    download(md)
                # TODO poll_status shouldn't have implementation details about callbacks
                if metadata_dict:
                    metadata = \
                        DeployedInferenceMetadata.from_dict(metadata_dict)
                    create_deployed_inference(metadata)
                break

        time.sleep(60)


app.conf.task_routes = {'*.gcp_celery.*': {'queue': 'gcp_celery'}}

'''
celery --app=train.gcp_celery worker -Q gcp_celery -l info -n gcp_celery
'''
