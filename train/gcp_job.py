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
from collections import namedtuple
from pathlib import Path
from typing import List, Optional
from db.utils import get_local_data_file_path
from .paths import _get_version_dir
from .no_deps.paths import (
    _get_config_fname,
    _get_exported_data_fname
)
from .no_deps.utils import (
    gs_copy_file,
    run_cmd
)

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


def _fmt_yaml_list(key, values: List[str], nspaces=0):
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
        files_for_inference: A list of files we would like to run inference on.
        docker_image_uri: The docker image URI. If None, will default to the
            env var GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI.
        label_type: Label for type.
        label_owner: Label for who ran the model.
    """

    if docker_image_uri is None:
        docker_image_uri = os.environ.get(
            'GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI')
    assert docker_image_uri

    # Note: Spacing matters since we're constructing a yaml file.
    fmt_model_dirs = _fmt_yaml_list('dirs', model_dirs, nspaces=4)
    fmt_files_for_inference = _fmt_yaml_list(
        'infer', files_for_inference, nspaces=4)

    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "Missing GOOGLE_AI_PLATFORM_BUCKET"
    remote_data_dir = f'gs://{bucket}/data'

    # TODO simple formatting like this is subject to injection attack.
    return JOB_CONFIG_TEMPLATE.format(
        model_dirs=fmt_model_dirs,
        files_for_inference=fmt_files_for_inference,
        remote_data_dir=remote_data_dir,
        docker_image_uri=docker_image_uri,
        label_type=label_type,
        label_owner=label_owner,
        version=version)


def build_remote_model_dir(model_uuid, model_version):
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket, "Missing GOOGLE_AI_PLATFORM_BUCKET"
    # Legacy path system - let's not change it in fear of breaking things
    return f'gs://{bucket}/tasks/{model_uuid}/models/{model_version}'


def get_name(filename_or_path):
    return Path(filename_or_path).name


def build_remote_data_fname(data_filename):
    """
    Inputs:
        data_filename: e.g. spring_jan_2020.jsonl
    """
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket
    return f'gs://{bucket}/data/{get_name(data_filename)}'


def sync_data_file(fname):
    local_fname = get_local_data_file_path(fname)
    remote_fname = build_remote_data_fname(fname)
    gs_copy_file(local_fname, remote_fname, no_clobber=True)
    return remote_fname


def prepare_model_assets_for_training(model_uuid, model_version):
    # Training only needs the config and exported data.

    version_dir = _get_version_dir(model_uuid, model_version)
    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)

    # We assume these two files exist
    assert os.path.isfile(config_fname), \
        f'Config File Not Found: {config_fname}'
    assert os.path.isfile(data_fname), f'Data File Not Found: {data_fname}'

    output_dir = build_remote_model_dir(model_uuid, model_version)

    gs_copy_file(config_fname,
                 os.path.join(output_dir, Path(config_fname).name))
    gs_copy_file(data_fname,
                 os.path.join(output_dir, Path(data_fname).name))

    return output_dir


def download(model: ModelDefn, include_weights=False):
    """Download a model from cloud to local storage.
    Args:
        include_weights: If True, download the model weights as well.
    """
    src_dir = build_remote_model_dir(model.uuid, model.version)
    dst_dir = _get_version_dir(model.uuid, model.version)
    if include_weights:
        run_cmd(f'gsutil -m rsync -r {src_dir} {dst_dir}')
    else:
        run_cmd(f'gsutil -m rsync -x "model" -r {src_dir} {dst_dir}')


def submit_google_ai_platform_job(job_id, config_file):
    run_cmd(f'gcloud ai-platform jobs submit training {job_id}'
            f' --config {config_file}')


def submit_job(model_defns: List[ModelDefn],
               files_for_inference: Optional[List[str]] = None,
               force_retrain=False,
               submit_job_fn=None):
    """
    Returns:
        The job id on Google AI Platform.
    """
    # TODO pass through the force_retrain parameter.

    if submit_job_fn is None:
        submit_job_fn = submit_google_ai_platform_job

    # A valid job_id only contains letters, numbers and underscores
    # AND must start with a letter.
    job_id = 't_' + str(uuid.uuid4()).replace('-', '_')

    print("Upload model assets for training, if needed")
    model_dirs = [prepare_model_assets_for_training(md.uuid, md.version)
                  for md in model_defns]

    # Make sure the filenames are _names_, not _paths_
    files_for_inference = files_for_inference or []
    files_for_inference = [get_name(f) for f in files_for_inference]

    print("Upload data for inference, if needed")
    for fname in files_for_inference:
        sync_data_file(fname)

    print("Submit job")
    with tempfile.NamedTemporaryFile(mode="w") as fp:
        model_config = build_job_config(
            model_dirs=model_dirs,
            files_for_inference=files_for_inference)
        fp.write(model_config)
        fp.flush()

        submit_job_fn(job_id, fp.name)

    return job_id


class GoogleAIPlatformJob:
    def __init__(self, response: dict):
        """
        Note: Construct this via GoogleAIPlatformJob.fetch(job_id)

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
        """
        self.response = response or {}

    @classmethod
    def fetch(cls, job_id: str):
        cmd = f"gcloud ai-platform jobs describe {job_id} --format json"

        # TODO this is not best way to capture real error
        # vs when a status doens't exist.
        try:
            res = run_cmd(cmd)
        except Exception as e:
            print(e)
            return None
        else:
            return cls(json.loads(res.stdout))

    def get_state(self):
        return self.response.get('state')

    def get_model_defns(self) -> List[ModelDefn]:
        try:
            training_args = self.response['trainingInput']['args']
        except KeyError:
            # self.response['trainingInput']['args'] does not exist
            return []
        else:
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
