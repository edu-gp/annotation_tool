from alchemy.db.fs import models_dir
from alchemy.shared.utils import mkd


def get_model_dir(uuid, version):
    # TODO: Legacy function with side effect of creating a dir when abs=True.
    p = str(models_dir(None, as_path=True) / uuid / str(version))
    if abs:
        return mkd(p)
    return p
