import json
import os
from pathlib import Path

import pandas as pd
from google.cloud import storage

from alchemy.db.fs import bucket


def _get_blob(fname):
    # if fname[0] == '/':
    #     fname = 'test/' + fname[1:]
    blob = storage.Blob(fname, bucket=bucket())
    return blob


def file_exists(file, data_store):
    if data_store == 'local':
        if not isinstance(file, Path):
            file = Path(file)
        else:
            file = file
        return file.exists()

    elif data_store == 'cloud':
        if not isinstance(file, storage.Blob):
            blob = _get_blob(file)
        else:
            blob = file
        return blob.exists()

    else:
        raise ValueError(f"Invalid data store {data_store}")


def listdir(dirname, data_store):
    if data_store == 'local':
        return os.listdir(dirname)
    elif data_store == 'cloud':
        client = storage.client.Client()
        return [blob.name for blob in
                client.list_blobs(bucket(), prefix=dirname)
                if blob.name != dirname]
    else:
        raise ValueError(f"Invalid data store {data_store}")


def load_json(fname, data_store):
    if not file_exists(fname, data_store=data_store):
        return None
    if data_store == 'local':
        with open(fname, 'r') as file:
            content = file.read()
    elif data_store == 'cloud':
        blob = storage.Blob(fname, bucket())
        content = blob.download_as_text()
    else:
        raise ValueError(f"Invalid data store {data_store}")

    return json.loads(content)


def save_json(fname, data, data_store, **metadata):
    assert fname.endswith(".json")
    content = json.dumps(data)

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


def save_jsonl(fname, data, data_store, remove_local=False, **metadata):
    print(f"Saving jsonl fname={fname} remove_local={remove_local} metadata={metadata}")
    assert fname.endswith(".jsonl")

    if '/' in fname:
        directory = fname[:fname.rindex('/')]
        os.makedirs(directory, exist_ok=True)

    with open(fname, "w") as outfile:
        for entry in data:
            json.dump(entry, outfile)
            outfile.write("\n")

    if data_store == 'cloud':
        blob = _get_blob(fname)
        if metadata:
            if blob.metadata:
                blob.metadata.update(metadata)
            else:
                blob.metadata = metadata
        blob.upload_from_filename(fname)
        if remove_local:
            os.remove(fname)
