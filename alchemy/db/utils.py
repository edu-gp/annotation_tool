import functools
import json
import pathlib

from google.cloud import storage

from alchemy.db.fs import raw_data_dir, bucket_name
from ..shared.file_adapters import listdir


def _get_file_name(blob: storage.Blob):
    name = blob.name
    if '/' in name:
        name = name[name.rindex('/')+1:]
    return name


def _is_data_file_of_type(fname, file_type):
    if isinstance(fname, (str, pathlib.Path)):
        with open(fname) as f:
            try:
                data = json.loads(f.readline())
                return file_type in data
            except Exception:
                return False
    else:
        fname: storage.Blob
        if fname.metadata is None:
            return False
        return fname.metadata.get('data_type', None) == file_type


def _get_data_files_of_type(data_store, file_type):
    d = raw_data_dir()
    type_filter = functools.partial(_is_data_file_of_type, file_type=file_type)
    if data_store == 'cloud':
        client = storage.Client()
        return sorted(list(map(
            _get_file_name,
            filter(type_filter,
                   client.list_blobs(bucket_or_name=bucket_name(), prefix=d))
        )))
    elif data_store == 'local':
        return sorted([x for x in listdir(d, data_store=data_store) if type_filter(d / x)])
    else:
        raise ValueError(f"Invalid data store {data_store}")


def get_all_data_files(data_store):
    """Return all data files in the data folder"""
    return _get_data_files_of_type(data_store=data_store, file_type='text')


def get_all_pattern_files(data_store):
    """Return all data files in the data folder"""
    return _get_data_files_of_type(data_store=data_store, file_type='pattern')
