import os
from envparse import env

RAW_DATA_DIR = "raw_data"
TRAINING_DATA_DIR = "training_data"
MODELS_DIR = "models"


def filestore_base_dir():
    return env('ALCHEMY_FILESTORE_DIR', default='__filestore')


def raw_data_dir():
    # TODO Refactor: Use this function in more places.
    # If you search `RAW_DATA_DIR`, you'll find many duplicate instances of
    # this function. Try to move them all into this file.
    return os.path.join(filestore_base_dir(), RAW_DATA_DIR)
