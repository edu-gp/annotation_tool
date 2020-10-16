import os
from pathlib import Path

from .utils import gs_copy_file, run_cmd


class ModelStorageManager:
    """A logical interface to a model's assets. A ModelStorageManager instance
    provides methods to work with a local version of the model's assets and its
    remote counterpart on GCS.

    Note: As of the current version, the ModelStorageManager does not know what
    is inside the model directory; it simply syncs the entire directory between
    a local dir and a remote dir. In future versions, we may consider giving
    ModelStorageManager more responsibilities.

    To see what's inside a model's directory, see paths.py
    """

    def __init__(self, remote_model_dir: str, local_model_dir: str):
        """
        Inputs:
            remote_model_dir: A gs:// url; the model's location on GCS.
            local_model_dir: Path to the model's local directory.
        """
        self.remote_dir = remote_model_dir
        self.local_dir = local_model_dir

    def upload(self):
        """Upload the entire model folder, only contents that have changed"""
        run_cmd(f"gsutil -m rsync -r {self.local_dir} {self.remote_dir}")

    def download(self, include_weights=True):
        """Download a model from cloud to local storage.
        Args:
            include_weights: If True, download the model weights as well.
        """
        src = self.remote_dir
        dst = self.local_dir
        if include_weights:
            run_cmd(f"gsutil -m rsync -r {src} {dst}")
        else:
            run_cmd(f'gsutil -m rsync -x "model" -r {src} {dst}')


class DatasetStorageManager:
    """Similar in principle to the ModelStorageManager, but this is for dealing
    with datasets"""

    def __init__(self, remote_data_dir, local_data_dir):
        """
        Inputs:
            remote_data_dir: A gs:// url; the raw dataset dir on GCS.
            local_data_dir: Path to the local directory that stores datasets.
        """
        self.remote_dir = remote_data_dir
        self.local_dir = local_data_dir

    def sync(self, dataset):
        """If the dataset exists at least either locally or remotely, this
        method makes sure the dataset exists in both places."""
        # Make sure dataset is a file name, not a path.
        dataset = Path(dataset).name

        remote_path = f"{self.remote_dir}/{dataset}"
        local_path = f"{self.local_dir}/{dataset}"

        # Sync; Attempt download, then attempt upload.
        gs_copy_file(remote_path, local_path, no_clobber=True)
        gs_copy_file(local_path, remote_path, no_clobber=True)

        # Make sure it exists locally.
        assert os.path.isfile(local_path), f"Missing dataset: {local_path}"

    def download(self, dataset):
        # Make sure dataset is a file name, not a path.
        dataset = Path(dataset).name

        remote_path = f"{self.remote_dir}/{dataset}"
        local_path = f"{self.local_dir}/{dataset}"

        gs_copy_file(remote_path, local_path, no_clobber=True)

        return local_path
