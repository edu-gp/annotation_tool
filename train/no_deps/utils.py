import subprocess
import shlex
from scipy.special import softmax
import numpy as np
import os
import json
import pandas as pd

BINARY_CLASSIFICATION = 'binary'
MULTILABEL_CLASSIFICATION = 'multilabel'


# TODO: Move load_jsonl and load_original_data_text out of here.

# NOTE: This is copied from shared/utils.py,
# so we can break dependency to that module.
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


def load_original_data_text(datafname):
    text = load_jsonl(datafname)['text']
    text = text.fillna('')
    text = list(text)
    return text


def get_env_int(key, default):
    val = os.environ.get(key, default)
    if not isinstance(val, int):
        val = int(val)
    return val


def get_env_bool(key, default):
    val = os.environ.get(key, default)
    if isinstance(val, str):
        val = val.lower() in ['t', 'true', '1', 'y', 'yes']
    if not isinstance(val, bool):
        val = bool(val)
    return val


def _parse_labels(data):
    '''
    Inputs:
        data: [ 
            {
                'labels': {
                    'LABEL_1': 1,
                    'LABEL_2': -1,
                }
                ...
            },
            ...
        ]
    '''

    y = []
    problem_type = None
    class_order = []

    all_labels = set()
    for row in data:
        for label in row['labels']:
            all_labels.add(label)

    class_order = sorted(list(all_labels))

    if len(class_order) > 0:

        if len(class_order) == 1:
            problem_type = BINARY_CLASSIFICATION

            y = []

            for row in data:
                val = list(row['labels'].values())[0]
                if val == 1:
                    y.append(1)
                elif val == -1:
                    y.append(0)

        else:
            problem_type = MULTILABEL_CLASSIFICATION

            y = []

            class_order_idx = {k: i for i, k in enumerate(class_order)}

            for row in data:
                _y = [0] * len(class_order)
                for label in row['labels']:
                    if row['labels'][label] == 1:
                        _y[class_order_idx[label]] = 1
                y.append(_y)

    return y, problem_type, class_order


def raw_to_pos_prob(raw):
    """Raw model output to positive class probability"""
    probs_pos_class = []
    for out in raw:
        out = np.array(out)
        if len(out.shape) == 1:
            # This is typical style of outputs.
            probs_pos_class.append(softmax(out)[1])
        elif len(out.shape) == 2:
            # This is the style of outputs when we use sliding windows.
            # Take the average prob of all the window predictions.
            _prob = softmax(out, axis=1)[:, 1].mean()
            probs_pos_class.append(_prob)
        else:
            raise Exception(
                f"Unclear how to deal with raw dimension: {out.shape}")
    return probs_pos_class


def run_cmd(cmd: str):
    """Run a command line command
    Inputs:
        cmd: A command line command.
    """
    # check=True makes this function raise an Exception if the command fails.
    return subprocess.run(shlex.split(cmd), check=True, capture_output=True)


def gs_copy_dir(src_dir, dst_dir):
    # run_cmd(f'gsutil -m cp -r {src_dir}/* {dst_dir}')
    run_cmd(f'gsutil -m rsync -r {src_dir} {dst_dir}')


def gs_copy_file(fname, dst):
    run_cmd(f'gsutil cp {fname} {dst}')
