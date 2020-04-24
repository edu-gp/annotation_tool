import os

RAW_DATA_DIR = 'raw_data'
TRAINING_DATA_DIR = 'training_data'
MODELS_DIR = 'models'


def filestore_base_dir():
    return os.environ.get('ALCHEMY_FILESTORE_DIR', '__filestore')
