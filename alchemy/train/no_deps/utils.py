import json
import os
import shlex
import subprocess
from pathlib import Path

import numpy as np
import pandas as pd
from google.cloud import storage
from scipy.special import softmax
from sklearn.model_selection import train_test_split

BINARY_CLASSIFICATION = "binary"
MULTILABEL_CLASSIFICATION = "multilabel"


# TODO: Move load_jsonl and load_original_data_text out of here.

# TODO: copy the final version here
# NOTE: This is copied from shared/utils.py,
# so we can break dependency to that module.

def _get_blob(fname):
    if fname.startswith("gs://"):
        return storage.Blob.from_string(fname)
    else:
        blob = storage.Blob(fname, bucket=storage.Bucket(storage.Client(), 'alchemy-staging'))
    return blob


def load_jsonl(jsonl_fname, data_store, to_df=True):
    if data_store == 'local':
        if not isinstance(jsonl_fname, Path):
            jsonl_fname = Path(jsonl_fname)
        if not jsonl_fname.exists():
            return None
        content = jsonl_fname.read_text()
    elif data_store == 'cloud':
        blob = _get_blob(jsonl_fname)
        if not blob.exists():
            return None
        content = blob.download_as_text()
    else:
        raise ValueError(f"Invalid data store {data_store}")

    data = [json.loads(line) for line in content.split('\n') if line]
    if to_df:
        data = pd.DataFrame(data)
    return data


def save_jsonl(jsonl_fname, data, data_store, remove_local=False, **metadata):
    assert jsonl_fname.endswith(".jsonl")
    assert data_store == 'local'

    if '/' in jsonl_fname:
        directory = jsonl_fname[:jsonl_fname.rindex('/')]
        os.makedirs(directory, exist_ok=True)

    with open(jsonl_fname, "w") as outfile:
        for entry in data:
            json.dump(entry, outfile)
            outfile.write("\n")


def load_original_data_text(datafname, data_store):
    text = load_jsonl(datafname, data_store=data_store, to_df=True)["text"]
    text = text.fillna("")
    text = list(text)
    return text


def load_text_file(fname: str, data_store: str) -> str:
    if not file_exists(fname, data_store=data_store):
        return None
    if data_store == 'local':
        with open(fname, 'r') as file:
            content = file.read()
    elif data_store == 'cloud':
        blob = storage.Blob(fname, bucket=storage.Bucket(storage.Client(), 'alchemy-staging'))
        content = blob.download_as_text()
    else:
        raise ValueError(f"Invalid data store {data_store}")

    return content


def save_text_file(fname, content, data_store, **metadata):
    if data_store == 'local':
        if not isinstance(fname, Path):
            fname = Path(fname)
        fname.parent.mkdir(parents=True, exist_ok=True)
        fname.write_text(content)
    elif data_store == 'cloud':
        blob = _get_blob(fname)
        if metadata:
            if blob.metadata:
                blob.metadata.update(metadata)
            else:
                blob.metadata = metadata
        blob.upload_from_string(content)
    else:
        raise ValueError(f"Invalid data store {data_store}")


def load_json(fname, data_store):
    if not file_exists(fname, data_store=data_store):
        return None
    content = load_text_file(fname, data_store)

    return json.loads(content)


def save_json(fname, data, data_store, **metadata):
    assert fname.endswith(".json")
    content = json.dumps(data)

    save_text_file(fname=fname, content=content, data_store=data_store, **metadata)


def file_exists(file, data_store):
    if data_store == 'local':
        if not isinstance(file, Path):
            file = Path(file)
        else:
            file = file
        return file.exists()
    elif data_store == 'cloud':
        if isinstance(file, storage.Blob):
            blob = file
        else:
            file = str(file)
            if file.startswith("gs://"):
                blob = storage.Blob.from_string(file, client=storage.Client())
            else:
                blob = storage.Blob(file, bucket=storage.Bucket(storage.Client(), 'alchemy-staging'))
        return blob.exists()
    else:
        raise ValueError(f"Invalid data store {data_store}")


def _parse_labels(data):
    """
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
    """

    y = []
    problem_type = None
    class_order = []

    all_labels = set()
    for row in data:
        for label in row["labels"]:
            all_labels.add(label)

    class_order = sorted(list(all_labels))

    if len(class_order) > 0:

        if len(class_order) == 1:
            problem_type = BINARY_CLASSIFICATION

            y = []

            for row in data:
                val = list(row["labels"].values())[0]
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
                for label in row["labels"]:
                    if row["labels"][label] == 1:
                        _y[class_order_idx[label]] = 1
                y.append(_y)

    return y, problem_type, class_order


def _prepare_data(config_fname, data_fname, data_store):
    """
    Inputs:
        config_name: Full path to the config json
        data_fname: Full path to the data jsonl
    """
    config = load_json(config_fname, data_store=data_store)
    data = load_jsonl(data_fname, data_store=data_store, to_df=False)

    X = [x["text"] for x in data]
    y, problem_type, class_order = _parse_labels(data)

    # Train test split
    if config.get("test_size", 0) > 0:
        print("Train / Test split...")
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=config["test_size"],
            random_state=config.get("random_state", 42),
        )
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
            raise Exception(f"Unclear how to deal with raw dimension: {out.shape}")
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
        output = subprocess.run(shlex.split(cmd), check=True, capture_output=True)
        print("stdout:", str(output.stdout, encoding='utf-8'))
        print("stderr:", str(output.stderr, encoding='utf-8'))
        return output
    except subprocess.CalledProcessError as e:
        print("stdout:", str(e.stdout, encoding='utf-8'))
        print("stderr:", str(e.stderr, encoding='utf-8'))
        raise


def gs_copy_dir(src_dir, dst_dir, rsync_args=""):
    # run_cmd(f'gsutil -m cp -r {src_dir}/* {dst_dir}')
    run_cmd(f"gsutil -m rsync {rsync_args} -r {src_dir} {dst_dir}")


def gs_copy_file(fname, dst, no_clobber=True):
    source_blob = destination_blob = None

    client = storage.Client()
    if fname.startswith('gs://'):
        source_blob = storage.Blob.from_string(fname, client=client)
    if dst.startswith('gs://'):
        destination_blob = storage.Blob.from_string(dst, client=client)

    if source_blob and destination_blob:
        if no_clobber and destination_blob.exists():
            return
        source_blob.bucket.copy_blob(
            blob=source_blob,
            destination_bucket=destination_blob.bucket,
            new_name=destination_blob.name,
        )
    elif destination_blob and not source_blob:
        if no_clobber and destination_blob.exists():
            return
        destination_blob.upload_from_filename(fname)
    elif source_blob and not destination_blob:
        if no_clobber and Path(dst).exists():
            return
        source_blob.download_to_filename(dst)


def gs_exists(gs_url):
    """Check if gs_url points to a valid file we can access"""
    return file_exists(gs_url, data_store='cloud')


def save_file_numpy(filename, data, data_store='local', numpy_kwargs=dict()):
    np.save(filename, data, **numpy_kwargs)
    if data_store == 'cloud':
        blob = storage.Blob(filename, storage.Bucket(storage.Client(), 'alchemy-staging'))
        blob.upload_from_filename(filename)


def load_file_numpy(filename, data_store='local', numpy_kwargs=dict()):
    if data_store == 'cloud':
        blob = storage.Blob(filename, storage.Bucket(storage.Client(), 'alchemy-staging'))
        blob.download_to_filename(filename)
    if data_store == 'local' and not os.path.isfile(filename):
        return None

    return np.load(filename, **numpy_kwargs)


def listdir(dirname, type='local'):
    assert type == 'local'
    return os.listdir(dirname)
