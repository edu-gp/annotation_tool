import hashlib
import json
import logging
import os
import random
import uuid
from collections import defaultdict, Counter
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import typing


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


def stem(fname, include_suffix=False):
    """/blah/my_file.json.gz --> my_file"""
    path = Path(fname)
    stem = path.stem

    # If a filename has multiple suffixes, take them all off.
    stem = stem[:stem.index('.')] if '.' in stem else stem

    if include_suffix:
        stem = stem + ''.join(path.suffixes)

    return stem


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
    except Exception as e:
        logging.error(e)
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


def json_lookup(json_data, key):
    """
    Give a key "a.b.c", look up json_data['a']['b']['c']
    Returns None if any of the keys were not found.
    """
    sofar = json_data
    for k in key.split('.'):
        try:
            sofar = sofar[k]
        except:
            return None
    return sofar


def build_counter(annos: List[Optional[int]]):
    """
    Input is a list of annotation values \in {-1, 0, 1, nan}.
    We ignore 0 and nan, and return a Counter of {-1, 1}.
    """
    # Ignore all the elements that are 0 or nan.
    annos = [x for x in annos if x != 0 and not pd.isna(x)]
    return Counter(annos)


def get_entropy(annos: List[Optional[int]], eps=0.0001):
    """Contentiousness measured by entropy"""
    cnt = build_counter(annos)

    total = sum(cnt.values()) + eps
    probs = [cnt[x] / total for x in cnt]
    log_probs = [np.log(p + eps) for p in probs]
    entropy = -sum([p * logp for p, logp in zip(probs, log_probs)])
    return entropy


@dataclass
class WeightedVote:
    value: 'typing.Any'
    weight: float


def get_weighted_majority_vote(annos: List[WeightedVote],
                               invalid_values: Optional[typing.Tuple] = (0, -2, None)):
    """Calculate the weighted majority votes.

    :param annos: a list of WeightedVote(value, weight) objects
    :param invalid_values: a list of invalid vote values to exclude
    :return: the value with the highest weight
    """
    max_weight = -1
    max_weight_vote = None

    valid_votes = []
    for anno in annos:
        if __is_valid(anno, invalid_values):
            valid_votes.append(anno)

    if len(valid_votes) > 0:
        votes_count = defaultdict(float)

        for vote in valid_votes:
            votes_count[vote.value] += vote.weight
            if votes_count[vote.value] > max_weight:
                max_weight = votes_count[vote.value]
                max_weight_vote = vote.value

    return max_weight_vote


def __is_valid(anno, invalid_values):
    if anno is None:
        return False
    if pd.isna(anno):
        return False
    if anno.value in invalid_values and not pd.isna(anno.weight):
        return False
    return True
