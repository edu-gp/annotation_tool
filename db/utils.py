import os
import json
from db.fs import filestore_base_dir, RAW_DATA_DIR


def is_data_file(fname):
    with open(fname) as f:
        try:
            data = json.loads(f.readline())
            return 'text' in data
        except Exception:
            return False


def is_pattern_file(fname):
    with open(fname) as f:
        try:
            data = json.loads(f.readline())
            return 'pattern' in data
        except Exception:
            return False


def get_all_data_files():
    '''Return all data files in the data folder'''
    d = os.path.join(filestore_base_dir(), RAW_DATA_DIR)
    return sorted([x for x in os.listdir(d)
                   if is_data_file(os.path.join(d, x))])


def get_all_pattern_files():
    '''Return all data files in the data folder'''
    d = os.path.join(filestore_base_dir(), RAW_DATA_DIR)
    return sorted([x for x in os.listdir(d)
                   if is_pattern_file(os.path.join(d, x))])
