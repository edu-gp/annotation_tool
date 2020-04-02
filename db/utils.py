import os
from db import _data_dir


def get_all_data_files():
    '''Return all files in the data folder'''
    # NOTE I don't think this should be responsible for checking supported suffixes
    fnames = sorted(os.listdir(_data_dir()))
    return fnames
