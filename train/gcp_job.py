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
    - "--dir"
    - gs://_REDACTED_/tasks/8a79a035-56fa-415c-8202-9297652dfe75/models/6
    - "--infer"
    - gs://_REDACTED_/data/spring_jan_2020.jsonl
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
from pathlib import Path
from typing import List
from .paths import _get_version_dir
from .no_deps.paths import (
    _get_config_fname,
    _get_exported_data_fname
)
from .no_deps.utils import (
    gs_copy_file,
    run_cmd
)

VERSION = '1'

# n1-standard-4 + NVIDIA_TESLA_P100 gives us the best bang for the buck.
JOB_CONFIG_TEMPLATE = '''
labels:
  type: "{label_type}"
  owner: "{label_owner}"
  version: "{version}"
trainingInput:
  scaleTier: CUSTOM
  masterType: n1-standard-4
  args:
    - "--dir"
    - {model_dir}{formatted_infer_filenames}
    - "--eval-batch-size"
    - '16'
  region: us-central1
  masterConfig:
    acceleratorConfig:
      count: '1'
      type: NVIDIA_TESLA_P100
    imageUri: "{docker_image_uri}"
'''


def build_job_config(
        model_dir: str,
        infer_filenames: List[str] = None,
        docker_image_uri: str = None,
        label_type: str = 'production',
        label_owner: str = 'alchemy',
        version: str = VERSION):
    """
    Inputs:
        model_dir: The gs:// location of the model (Also known as the
            "version_dir" elsewhere in the codebase).
        infer_filenames: A list of gs:// locations for the files we would like
            to run inference on after training.
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
    formatted_infer_filenames = ''
    if isinstance(infer_filenames, list) and len(infer_filenames) > 0:
        formatted_infer_filenames = []
        formatted_infer_filenames.append('\n    - "--infer"')
        for fname in infer_filenames:
            formatted_infer_filenames.append(f'    - {fname}')
        formatted_infer_filenames = '\n'.join(formatted_infer_filenames)

    # TODO simple formatting like this is subject to injection attack.
    return JOB_CONFIG_TEMPLATE.format(
        model_dir=model_dir,
        formatted_infer_filenames=formatted_infer_filenames,
        docker_image_uri=docker_image_uri,
        label_type=label_type,
        label_owner=label_owner,
        version=version)


def build_remote_model_dir(model_uuid, model_version):
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket
    # Legacy path system - let's not change it in fear of breaking things
    return f'gs://{bucket}/tasks/{model_uuid}/models/{model_version}'


def build_remote_data_fname(data_filename):
    """
    Inputs:
        data_filename: e.g. spring_jan_2020.jsonl
    """
    bucket = os.environ.get('GOOGLE_AI_PLATFORM_BUCKET')
    assert bucket
    name = Path(data_filename).name
    return f'gs://{bucket}/data/{name}'


def get_exp_id(model_uuid, model_version):
    # Generate a Exp ID that hopefully is (likely to be) unique.
    # Note: They don't allow '-' in the name, just '_'.
    return f't_{model_uuid.replace("-", "_")}_v_{model_version}'


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


class GCPJob:
    """Remote job on the Google AI Platform
    This can be used either for training or batch inference.
    """

    def __init__(self, model_uuid, model_version):
        self.model_uuid = model_uuid
        self.model_version = model_version

    def download(self):
        # Download everything except the 'model' dir to save space.
        src_dir = build_remote_model_dir(self.model_uuid, self.model_version)
        dst_dir = _get_version_dir(self.model_uuid, self.model_version)
        run_cmd(f'gsutil -m rsync -x "model" -r {src_dir} {dst_dir}')

    def get_status(self):
        """
        Example response:
        {
            "createTime": "2020-04-02T23:24:18Z",
            "etag": "2TFzYuw9IIA=",
            "jobId": "t_99e5cb31_8343_4ec3_8b5e_c6cdedfb7e3d_v_5",
            "labels": {
                "owner": "alchemy",
                "type": "production",
                "version": "1"
            },
            "startTime": "2020-04-02T23:27:09Z",
            "state": "RUNNING",
            "trainingInput": {
                "args": [
                    "--dir",
                    "gs://_REDACTED_/tasks/99e5cb31-8343-4ec3-8b5e-c6cdedfb7e3d/models/5",
                    "--infer",
                    "gs://_REDACTED_/data/spring_jan_2020.jsonl",
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

        # TODO also use logs
        # gcloud ai-platform jobs stream-logs {exp_id}

        exp_id = get_exp_id(self.model_uuid, self.model_version)
        cmd = f"gcloud ai-platform jobs describe {exp_id} --format json"

        # TODO this is not best way to capture real error
        # vs when a status doens't exist.
        try:
            res = run_cmd(cmd)
        except Exception as e:
            print(e)
            return None
        else:
            return json.loads(res.stdout)

    def submit(self, files_for_inference=None):
        print("Upload model assets for training")
        model_dir = prepare_model_assets_for_training(
            self.model_uuid, self.model_version)

        print("Upload data for inference")
        infer_filenames = []
        for fname in files_for_inference or []:
            remote_fname = build_remote_data_fname(fname)
            # TODO check file does not already exist remotely
            gs_copy_file(fname, remote_fname)
            infer_filenames.append(remote_fname)

        print("Submit training job")
        with tempfile.NamedTemporaryFile(mode="w") as fp:
            model_config = build_job_config(
                model_dir=model_dir,
                infer_filenames=infer_filenames)
            fp.write(model_config)
            fp.flush()

            exp_id = get_exp_id(self.model_uuid, self.model_version)

            cmd = f'gcloud ai-platform jobs submit training {exp_id} --config {fp.name}'
            run_cmd(cmd)


"""
from train.gcp_job import GCPJob
job = GCPJob('99e5cb31-8343-4ec3-8b5e-c6cdedfb7e3d', 5)
job.submit()
job.get_status()
job.download()

from train.gcp_job import GCPJob
job = GCPJob('8a79a035-56fa-415c-8202-9297652dfe75', 6)
job.submit()
"""
