import os
from pathlib import Path
import numpy as np
import pandas as pd
import json


def load_jsonl(jsonl_fname, to_df=True):
    data = []
    with open(jsonl_fname) as f:
        for line in f:
            data.append(json.loads(line))
    if to_df:
        data = pd.DataFrame(data)
    return data

def save_jsonl(fname, data):
    assert fname.endswith('.jsonl')
    with open(fname, 'w') as outfile:
        for entry in data:
            json.dump(entry, outfile)
            outfile.write('\n')

def mkf(*file_path):
    '''Return file path, make sure the parent dir exists'''
    file_path = [str(x) for x in file_path]
    d = os.path.join(*file_path[:-1])
    os.makedirs(d, exist_ok=True)
    f = os.path.join(*file_path)
    return f

def mkd(*dir_path):
    '''Return dir path, make sure it exists'''
    dir_path = [str(x) for x in dir_path]
    d = os.path.join(*dir_path)
    os.makedirs(d, exist_ok=True)
    return d
