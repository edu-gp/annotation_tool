from shared.utils import mkd
from db.fs import filestore_base_dir, MODELS_DIR


def _get_version_dir(uuid, version):
    # TODO: Rename to get_model_dir
    return mkd(filestore_base_dir(), MODELS_DIR, uuid, str(version))
