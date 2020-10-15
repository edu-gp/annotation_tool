"""
Interface with Google AI Platform Job, to be used for training and inference.

Example Job Config:

'''
labels:
  type: dev
  owner: eddie
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dirs"
    - gs://_REDACTED_/tasks/8a79a035-56fa-415c-8202-9297652dfe75/models/6
    - "--data-dir"
    - gs://_REDACTED_/data
    - "--infer"
    - spring_jan_2020.jsonl
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: gcr.io/_REDACTED_
'''
"""

import os
import json
import tempfile
import uuid
import re
from envparse import env
from collections import namedtuple
from pathlib import Path
from typing import List, Optional
from alchemy.train import gs_url
from alchemy.db.fs import raw_data_dir
from .paths import _get_version_dir
from .no_deps.utils import run_cmd
from .no_deps.storage_manager import DatasetStorageManager, ModelStorageManager

ModelDefn = namedtuple("ModelDefn", ("uuid", "version"))

VERSION = '2'

# n1-standard-4 + NVIDIA_TESLA_P100 gives us the best bang for the buck.
JOB_CONFIG_TEMPLATE = '''
labels:
  type: "{label_type}"
  owner: "{label_owner}"
  version: "{version}"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:{model_dirs}{files_for_inference}
    - "--data-dir"
    - {remote_data_dir}
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "{docker_image_uri}"
'''


def __fmt_yaml_list(key, values: List[str], nspaces=0):
    """Spacing matters since we're constructing a yaml file. This function
    creates a string that represents a list of values in yaml."""
    result = ''
    if isinstance(values, list) and len(values) > 0:
        result = []
        prefix = ' '*nspaces
        result.append(f'{prefix}- "--{key}"')
        for v in values:
            result.append(f'{prefix}- {v}')
        result = '\n' + '\n'.join(result)
    return result


def build_job_config(
        model_dirs: List[str],
        files_for_inference: List[str] = None,
        docker_image_uri: str = None,
        label_type: str = 'production',
        label_owner: str = 'alchemy',
        version: str = VERSION):
    """
    Inputs:
        model_dirs: The list of gs:// location of the models (Also known as the
            "version_dir" elsewhere in the codebase).
        files_for_inference: A list of datasets to run inference on, can
            either be the dataset names OR their gs:// urls.
        docker_image_uri: The docker image URI. If None, will default to the
            env var GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI.
        label_type: Label for type.
        label_owner: Label for who ran the model.
    """

    if docker_image_uri is None:
        docker_image_uri = env('GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI')
    assert docker_image_uri

    # Format lists into proper yaml.
    formatted_model_dirs = __fmt_yaml_list('dirs', model_dirs, nspaces=4)
    formatted_files_for_inference = __fmt_yaml_list(
        'infer', files_for_inference, nspaces=4)

    return JOB_CONFIG_TEMPLATE.format(
        model_dirs=formatted_model_dirs,
        files_for_inference=formatted_files_for_inference,
        remote_data_dir=gs_url.build_raw_data_dir(),
        docker_image_uri=docker_image_uri,
        label_type=label_type,
        label_owner=label_owner,
        version=version)


class GoogleAIPlatformJob:
    """Encapsulate an AI Platform job and provide some useful methods"""

    def __init__(self, job_id: str):
        self.id = job_id
        self.data_ = None

    def get_data(self) -> dict:
        if self.data_ is None:
            try:
                self.data_ = describe_ai_platform_job(self.id)
            except:
                # If we run into an error, we'll retry on the next call.
                pass
        # If we ran into an error above, self.data_ would still be None.
        # We want to always return a dict, so in the worst case we return {}.
        return self.data_ or {}

    def get_state(self):
        return self.get_data().get('state')

    def get_model_defns(self) -> List[ModelDefn]:
        try:
            training_args = self.get_data()['trainingInput']['args']
        except KeyError:
            # self.get_data()['trainingInput']['args'] does not exist
            return []
        else:
            # Parse the `training_args` str to find all the model directories.
            res = []
            in_region = False

            for el in training_args:
                if in_region:
                    if el.startswith('--'):
                        # We've exited the region with model dirs.
                        in_region = False
                        # There should be no more.
                        break
                    else:
                        res.append(el)
                else:
                    # v2 is called --dirs, v1 is called --dir
                    if el == '--dirs' or el == '--dir':
                        # We hit the region with model dirs.
                        in_region = True

            model_defns = []
            for remote_model_dir in res:
                # remote_model_dir is of the form:
                # "gs://_REDACTED_/tasks/8b5e-c6cdedfb7e3d/models/6"
                # This parses out "8b5e-c6cdedfb7e3d" and "6"
                m = re.match(".+/(.+)/models/(.+)", remote_model_dir)
                md = ModelDefn(*m.groups())
                model_defns.append(md)

            return model_defns

    def cancel(self):
        """Cancel a running job
        Raises:
            Exception if a job has already completed
        """
        cancel_ai_platform_job(self.id)


def submit_job(model_defns: List[ModelDefn],
               files_for_inference: Optional[List[str]] = None,
               force_retrain: bool = False,
               submit_job_fn: callable = None) -> GoogleAIPlatformJob:
    """Submits a job and returns the corresponding GoogleAIPlatformJob."""
    # TODO pass through the force_retrain parameter.

    # Make sure each dataset is a file name, not a path.
    datasets_for_inference = [Path(dataset).name
                              for dataset in files_for_inference]

    print("Upload model assets for training")
    gcs_model_dirs = []
    for md in model_defns:
        msm = build_model_storage_manager(md.uuid, md.version)
        msm.upload()
        gcs_model_dirs.append(msm.remote_dir)

    print("Sync data for inference")
    dsm = build_dataset_storage_manager()
    for dataset in datasets_for_inference:
        # Ensure each dataset exists locally _and_ on GCS.
        # TODO: We need datasets locally because later on we'll need it to
        #       compute metrics, export inferences, etc. However, it's a bit
        #       strange to download datasets here, when we really should only
        #       be uploading datasets.
        dsm.sync(dataset)

    job_id = __generate_job_id()
    print(f"Submit job: {job_id}")

    model_config = build_job_config(
        model_dirs=gcs_model_dirs,
        files_for_inference=datasets_for_inference)

    with tempfile.NamedTemporaryFile(mode="w") as fp:
        fp.write(model_config)
        fp.flush()

        if submit_job_fn is None:
            submit_job_fn = submit_ai_platform_job
        submit_job_fn(job_id, fp.name)

    return GoogleAIPlatformJob(job_id)


def __generate_job_id():
    # A valid job_id only contains letters, numbers and underscores,
    # AND must start with a letter.
    return 't_' + str(uuid.uuid4()).replace('-', '_')


def build_model_storage_manager(uuid, version) -> ModelStorageManager:
    """Factory function"""
    remote_model_dir = gs_url.build_model_dir(uuid, version)
    local_model_dir = _get_version_dir(uuid, version)
    return ModelStorageManager(remote_model_dir, local_model_dir)


def build_dataset_storage_manager() -> DatasetStorageManager:
    """Factory function"""
    remote_data_dir = gs_url.build_raw_data_dir()
    local_data_dir = raw_data_dir()
    return DatasetStorageManager(remote_data_dir, local_data_dir)


def submit_ai_platform_job(job_id, config_file):
    """
    Inputs:
        job_id: The id of the new job.
        config_file: Path to a config file on disk.
    """
    run_cmd(f'gcloud ai-platform jobs submit training {job_id}'
            f' --config {config_file}')


def describe_ai_platform_job(job_id: str) -> dict:
    """
    Given a job_id of an AI Platform job, return information about the job.

    Example response:
    {
        "createTime": "2020-04-02T23:24:18Z",
        "etag": "2TFzYuw9IIA=",
        "jobId": "t_99e5cb31_8343_4ec3_8b5e_c6cdedfb7e3d_v_5",
        "labels": {
            "owner": "alchemy",
            "type": "production",
            "version": "2"
        },
        "startTime": "2020-04-02T23:27:09Z",
        "state": "RUNNING",
        "trainingInput": {
            "args": [
                "--dirs",
                "gs://_REDACTED_/tasks/99e5cb31-8343-4ec3-8b5e-c6cdedfb7e3d/models/6",
                "gs://_REDACTED_/tasks/99e5cb31-8343-4ec3-8b5e-c6cdedfb7e3d/models/7",
                "--infer",
                "gs://_REDACTED_/data/spring_jan_2020.jsonl",
                "gs://_REDACTED_/data/spring_feb_2020.jsonl",
                "--eval-batch-size",
                "16"
            ],
            "masterConfig": {
                "acceleratorConfig": {
                    "count": "1",
                    "type": "NVIDIA_TESLA_P100"
                },
                "imageUri": "gcr.io/_REDACTED_"
            },
            "masterType": "n1-standard-4",
            "region": "us-central1",
            "scaleTier": "CUSTOM"
        },
        "trainingOutput": {}
    }

    For all states, see:
    https://cloud.google.com/ai-platform/training/docs/reference/rest/v1/projects.jobs#State
    """
    cmd = f"gcloud ai-platform jobs describe {job_id} --format json"
    res = run_cmd(cmd)
    return json.loads(res.stdout)


def cancel_ai_platform_job(job_id: str):
    run_cmd(f'gcloud ai-platform jobs cancel {job_id}')


if __name__ == '__main__':
    md = ModelDefn('229a971a-2a1c-47ec-9934-4e4abcef5bd6', '6')

    # Part 1. Submit job
    # '''
    # python -m train.gcp_job
    # '''

    # def dummy_submit_job(job_id, config_file):
    #     print(f"submitted job_id={job_id}")

    # submit_job([md],
    #            ['spring_jan_2020_small.jsonl',
    #             'spring_jan_2020_small_v2.jsonl'],
    #            submit_job_fn=dummy_submit_job)

    # Part 2. Train & Inference (simulating it locally)
    '''
    Then, you can run the training & inference locally (this would be what's triggered automatically on GCP):
    python -m train.no_deps.gcp_run --dirs gs://alchemy-gp/tasks/229a971a-2a1c-47ec-9934-4e4abcef5bd6/models/6 --data-dir gs://alchemy-gp/data --infer spring_jan_2020_small.jsonl spring_jan_2020_small_v2.jsonl --eval-batch-size 16
    '''

    # # Part 3. Download the result to local
    # from train.gcp_job import download
    # download(md)
