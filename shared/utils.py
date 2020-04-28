import hashlib
import os
from collections import defaultdict
import pandas as pd
import json
import uuid
from pathlib import Path
from typing import List


def load_json(fname):
    if os.path.isfile(fname):
        with open(fname) as f:
            return json.loads(f.read())
    else:
        return None


def save_json(fname, data):
    assert fname.endswith('.json')
    with open(fname, 'w') as outfile:
        json.dump(data, outfile)


def load_jsonl(jsonl_fname, to_df=True):
    if os.path.isfile(jsonl_fname):
        data = []
        with open(jsonl_fname) as f:
            for line in f:
                data.append(json.loads(line))
        if to_df:
            data = pd.DataFrame(data)
        return data
    else:
        return None


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


def get_env_int(key, default):
    val = os.environ.get(key, default)
    if not isinstance(val, int):
        val = int(val)
    return val


def stem(fname):
    """/blah/my_file.json.gz --> my_file"""
    stem = Path(fname).stem
    return stem[:stem.index('.')] if '.' in stem else stem


def generate_md5_hash(data: str):
    """Generate a md5 hash of the input str

    :param data: the input str
    :return: the md5 hash string
    """
    return hashlib.md5(data.encode()).hexdigest()


class PrettyDefaultDict(defaultdict):
    """An wrapper around defaultdict so the print out looks like
    a normal dict."""
    __repr__ = dict.__repr__


def gen_uuid():
    return str(uuid.uuid4())


def file_len(fname):
    try:
        with open(fname) as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    except:
        return -1


def safe_getattr(object, attr):
    try:
        return getattr(object, attr)
    except AttributeError:
        return None


def list_to_textarea(ls: List[str]):
    """Converts a list of strings to a string joined by newlines.
    This is meant to be used when rendering into a textarea.
    """
    return "\n".join(ls)


def textarea_to_list(text: str):
    """Converts a textarea into a list of strings, assuming each line is an
    item. This is meant to be the inverse of `list_to_textarea`.
    """
    res = [x.strip() for x in text.split('\n')]
    res = [x for x in res if len(x) > 0]
    return res
