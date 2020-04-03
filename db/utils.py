import os
import json
from db import _data_dir


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
    d = _data_dir()
    return sorted([x for x in os.listdir(d)
                   if is_data_file(os.path.join(d, x))])


def get_all_pattern_files():
    '''Return all data files in the data folder'''
    d = _data_dir()
    return sorted([x for x in os.listdir(d)
                   if is_pattern_file(os.path.join(d, x))])
