import logging
import os
from typing import Optional

from celery import Celery
from celery.signals import worker_init

from alchemy.train.gcp_job import GoogleAIPlatformJob, build_model_storage_manager
from alchemy.train.gs_utils import DeployedInferenceMetadata, create_deployed_inference

app = Celery(
    # module name
    "gcp_celery",
    # redis://:password@hostname:port/db_number
    broker=f"redis://{os.getenv('REDIS_HOST', 'localhost')}:6379/0",
    # # store the results here
    # backend='redis://localhost:6379/0',
)


@worker_init.connect
def on_init(*args, **kwargs):
    from pathlib import Path
    from flask import Config as FlaskConfigManager

    config = FlaskConfigManager(Path('../..').absolute())
    config.from_envvar('ALCHEMY_CONFIG')
    from alchemy.train import gs_url
    gs_url.GOOGLE_AI_PLATFORM_BUCKET = config['GOOGLE_AI_PLATFORM_BUCKET']


# See all states at: https://cloud.google.com/ai-platform/training/docs/reference/rest/v1/projects.jobs#State
RUNNING_STATES = ["QUEUED", "PREPARING", "RUNNING", "CANCELLING", "STATE_UNSPECIFIED"]
TERMINAL_STATES = ["SUCCEEDED", "FAILED", "CANCELLED"]


@app.task
def poll_status(job_id: str, metadata_dict: Optional[dict] = None, poll_count: int = 0):
    """
    Inputs:
        job_id: GoogleAIPlatformJob job id
        metadata: Production metadata. If None, then this run shouldn't go into
            the production bucket.
        poll_count: How many times did we poll already. Since we wait 60
            seconds between polls, this tells use how long the job has been
            running for, and we can use this to cancel runaway jobs.
    """
    logging.info(f"Poll AI Platform job_id={job_id}, poll_count={poll_count}")

    job = GoogleAIPlatformJob(job_id)

    if job is None:
        raise Exception("Unknown job")
    else:
        job_state = job.get_state()
        logging.info(f"job_state={job_state}")

        if job_state == "SUCCEEDED":
            logging.info(f"Done AI Platform job_id={job_id}")
            on_job_success(job, metadata_dict)
        elif poll_count > 60:
            # A job is taking too long. Try to cancel it.
            try:
                logging.info(f"Cancel AI Platform job_id={job_id}")
                job.cancel()
            except Exception as e:
                logging.error(f"Error in canceling job: {e}")
                # If an error occurred, we can try again later.
                pass

        if job_state in RUNNING_STATES:
            # Check again in 60 seconds
            poll_status.apply_async(
                args=[job_id, metadata_dict, poll_count + 1], countdown=60
            )


def on_job_success(job: GoogleAIPlatformJob, metadata_dict: Optional[dict] = None):
    # Sync down the model assets
    for md in job.get_model_defns():
        msm = build_model_storage_manager(md.uuid, md.version)
        msm.download(include_weights=False)
    # Deploy inferences to prod as needed
    if metadata_dict:
        metadata = DeployedInferenceMetadata.from_dict(metadata_dict)
        create_deployed_inference(metadata)


app.conf.task_routes = {"*.gcp_celery.*": {"queue": "gcp_celery"}}

"""
celery --app=train.gcp_celery worker -Q gcp_celery -l info -n gcp_celery
"""
