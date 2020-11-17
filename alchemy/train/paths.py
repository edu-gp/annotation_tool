import os

from alchemy.db.fs import models_dir


def mkd(*dir_path):
    """Return dir path, make sure it exists"""
    dir_path = [str(x) for x in dir_path]
    d = os.path.join(*dir_path)
    os.makedirs(d, exist_ok=True)
    return d


def get_model_dir(uuid, version, abs=True):
    # TODO: Legacy function with side effect of creating a dir when abs=True.
    if abs:
        base = None
    else:
        base = ''
    p = str(models_dir(base, as_path=True) / uuid / str(version))
    if abs:
        return mkd(p)
    return p
