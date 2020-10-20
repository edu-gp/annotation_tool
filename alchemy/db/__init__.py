import os

from alchemy.shared.config import Config
from alchemy.shared.utils import mkd


def _data_dir():
    d = Config.get_data_dir()
    mkd(d)
    return d


def _task_dir(task_id=None):
    d = Config.get_tasks_dir()
    mkd(d)

    if task_id:
        return os.path.join(d, task_id)
    else:
        return d
