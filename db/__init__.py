import os
from shared.utils import mkd

DEFAULT_DATA_STORAGE = '__data'
DEFAULT_TASK_STORAGE = '__tasks'
mkd(DEFAULT_DATA_STORAGE)
mkd(DEFAULT_TASK_STORAGE)

def _data_dir():
    return DEFAULT_DATA_STORAGE

def _task_dir(task_id=None):
    if task_id:
        return os.path.join(DEFAULT_TASK_STORAGE, task_id)
    else:
        return DEFAULT_TASK_STORAGE
