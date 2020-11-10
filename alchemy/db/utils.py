import json
import os
import pathlib

from google.cloud import storage

from alchemy.db.fs import raw_data_dir, bucket_name


def _get_file_name(blob: storage.Blob):
    name = blob.name
    if '/' in name:
        name = name[name.rindex('/')+1:]
    return name


def is_data_file(fname):
    if isinstance(fname, (str, pathlib.Path)):
        with open(fname) as f:
            try:
                data = json.loads(f.readline())
                return "text" in data
            except Exception:
                return False
    else:
        fname: storage.Blob
        if fname.metadata is None:
            return False
        return fname.metadata.get('data_type', None) == 'data'


def is_pattern_file(fname):
    if isinstance(fname, (str, pathlib.Path)):
        with open(fname) as f:
            try:
                data = json.loads(f.readline())
                return "pattern" in data
            except Exception:
                return False
    else:
        fname: storage.Blob
        if fname.metadata is None:
            return False
        return fname.metadata.get('data_type', None) == 'pattern'


def get_all_data_files():
    """Return all data files in the data folder"""
    d = raw_data_dir()
    client = storage.Client()
    return sorted(list(map(
        _get_file_name,
        filter(is_data_file,
               client.list_blobs(bucket_or_name=bucket_name(), prefix=d))
    )))


def get_all_pattern_files():
    """Return all data files in the data folder"""
    d = raw_data_dir()
    client = storage.Client()
    return sorted(list(map(
        _get_file_name,
        filter(is_pattern_file,
               client.list_blobs(bucket_or_name=bucket_name(), prefix=d))
    )))


def get_local_data_file_path(fname):
    return os.path.join(raw_data_dir(), fname)
