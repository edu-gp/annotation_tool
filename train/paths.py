import os
from shared.utils import mkd
from db import _task_dir


def _get_all_model_versions(task_id):
    _dir = os.path.join(_task_dir(task_id), 'models')
    if os.path.isdir(_dir):
        versions = []
        for dirname in os.listdir(_dir):
            try:
                versions.append(int(dirname))
            except:
                pass
        return versions
    else:
        return []


def _get_latest_model_version(task_id):
    versions = _get_all_model_versions(task_id)
    if len(versions) > 0:
        return max(versions)
    else:
        return 0


def _get_version_dir(task_id, version):
    return mkd(_task_dir(task_id), 'models', str(version))
