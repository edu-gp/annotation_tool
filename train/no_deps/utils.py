import subprocess
import shlex
from scipy.special import softmax
import numpy as np
import os
import json
import pandas as pd
from sklearn.model_selection import train_test_split

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


def _load_config(config_fname):
    config = None
    with open(config_fname) as f:
        config = json.loads(f.read())
    assert config, "Missing config"
    return config


def _prepare_data(config_fname, data_fname):
    """
    Inputs:
        config_name: Full path to the config json
        data_fname: Full path to the data jsonl
    """
    config = _load_config(config_fname)

    data = []
    with open(data_fname) as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))

    X = [x['text'] for x in data]
    y, problem_type, class_order = _parse_labels(data)

    # Train test split
    if config.get('test_size', 0) > 0:
        print("Train / Test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            X, y,
            test_size=config['test_size'],
            random_state=config.get('random_state', 42))
    else:
        print("No train test split used")
        X_train = X
        y_train = y
        X_test = None
        y_test = None

    return problem_type, class_order, X_train, y_train, X_test, y_test


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
    # print the command for easier debugging
    print(cmd)
    # check=True makes this function raise an Exception if the command fails.
    try:
        output = subprocess.run(shlex.split(
            cmd), check=True, capture_output=True)
        print("stdout:", output.stdout)
        print("stderr:", output.stderr)
        return output
    except subprocess.CalledProcessError as e:
        print("stdout:", e.stdout)
        print("stderr:", e.stderr)
        raise


def gs_copy_dir(src_dir, dst_dir):
    # run_cmd(f'gsutil -m cp -r {src_dir}/* {dst_dir}')
    run_cmd(f'gsutil -m rsync -r {src_dir} {dst_dir}')


def gs_copy_file(fname, dst, no_clobber=True):
    if no_clobber:
        # -n : No-clobber. When specified, existing files or objects at the destination will not be overwritten.
        run_cmd(f'gsutil cp -n {fname} {dst}')
    else:
        # Overwrite existing
        run_cmd(f'gsutil cp {fname} {dst}')


def gs_exists(gs_url):
    """Check if gs_url points to a valid file we can access"""
    # If gs_url is not accessible, the response could be one of:
    #   1. "You aren't authorized to read ..."
    #   2. "No URLs matched: ..."
    # and it would have a non-0 status, which would be raised.
    #
    # Otherwise, it would return a bunch of information about the file,
    # one of them being "Creation time".
    try:
        res = run_cmd(f'gsutil stat {gs_url}')
        return 'Creation time:' in res.stdout.decode('utf-8')
    except subprocess.CalledProcessError:
        return False
