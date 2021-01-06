import json
import os
import tempfile
import uuid
import re
from envparse import env
from collections import namedtuple
from pathlib import Path
from typing import List, Optional
import subprocess
import shlex

VERSION = 2

JOB_CONFIG_TEMPLATE = """
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
"""


def __fmt_yaml_list(key, values: List[str], nspaces=0):
    """Spacing matters since we're constructing a yaml file. This function
    creates a string that represents a list of values in yaml."""
    result = ""
    if isinstance(values, list) and len(values) > 0:
        result = []
        prefix = " " * nspaces
        result.append(f'{prefix}- "--{key}"')
        for v in values:
            result.append(f"{prefix}- {v}")
        result = "\n" + "\n".join(result)
    return result


def build_job_config(
    model_dirs: List[str],
    files_for_inference: List[str] = None,
    docker_image_uri: str = None,
    label_type: str = "production",
    label_owner: str = "alchemy",
    version: str = VERSION,
):
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
        docker_image_uri = env("GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI")
    assert docker_image_uri

    # Format lists into proper yaml.
    formatted_model_dirs = __fmt_yaml_list("dirs", model_dirs, nspaces=4)
    formatted_files_for_inference = __fmt_yaml_list(
        "infer", files_for_inference, nspaces=4
    )

    return JOB_CONFIG_TEMPLATE.format(
        model_dirs=formatted_model_dirs,
        files_for_inference=formatted_files_for_inference,
        remote_data_dir="gs://alchemy-gp/data",  # gs_url.build_raw_data_dir()
        docker_image_uri=docker_image_uri,
        label_type=label_type,
        label_owner=label_owner,
        version=version,
    )


def submit_ai_platform_job(job_id, config_file):
    """
    Inputs:
        job_id: The id of the new job.
        config_file: Path to a config file on disk.
    """
    run_cmd(
        f"gcloud ai-platform jobs submit training {job_id}" f" --config {config_file}"
    )


def __generate_job_id():
    # A valid job_id only contains letters, numbers and underscores,
    # AND must start with a letter.
    return "t_" + str(uuid.uuid4()).replace("-", "_")


def run_cmd(cmd: str):
    """Run a command line command
    Inputs:
        cmd: A command line command.
    """
    # print the command for easier debugging
    print(cmd)
    # check=True makes this function raise an Exception if the command fails.
    try:
        output = subprocess.run(shlex.split(cmd), check=True, capture_output=True)
        print("stdout:", output.stdout)
        print("stderr:", output.stderr)
        return output
    except subprocess.CalledProcessError as e:
        print("stdout:", e.stdout)
        print("stderr:", e.stderr)
        raise


if __name__ == "__main__":
    job_id = __generate_job_id()
    print(f"Submit job: {job_id}")

    gcs_model_dirs = [
        "gs://alchemy-gp/tasks/95f8fcad08680ac3167d20f315c09b987595d04f15feb16f34a38456/models/5"
    ]
    datasets_for_inference = ["spring_jan_2020_small.jsonl"]
    docker_image_uri = "gcr.io/nlp-flywheel/alchemy-exp@sha256:e21902f05d649512c2fd08d0f10f91d48334f012f34dee676fcd41bb4611eff5"
    model_config = build_job_config(
        model_dirs=gcs_model_dirs,
        files_for_inference=datasets_for_inference,
        docker_image_uri=docker_image_uri,
        label_owner="test",
        label_type="experimental",
    )

    print(model_config)

    submit_job_fn = None
    with tempfile.NamedTemporaryFile(mode="w") as fp:
        fp.write(model_config)
        fp.flush()

        if submit_job_fn is None:
            submit_job_fn = submit_ai_platform_job
        submit_job_fn(job_id, fp.name)

    print(job_id)
