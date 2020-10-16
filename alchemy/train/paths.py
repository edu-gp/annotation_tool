import os

from alchemy.db.fs import MODELS_DIR, filestore_base_dir
from alchemy.shared.utils import mkd


def _get_version_dir(uuid, version, abs=True):
    # TODO: Legacy function with side effect of creating a dir when abs=True.
    # TODO: Rename to get_model_dir
    if abs:
        return mkd(filestore_base_dir(), MODELS_DIR, uuid, str(version))
    else:
        return os.path.join(MODELS_DIR, uuid, str(version))
