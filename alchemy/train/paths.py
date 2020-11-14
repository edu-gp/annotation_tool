from alchemy.db.fs import models_dir


def get_model_dir(uuid, version):
    p = models_dir(None, as_path=True) / uuid / str(version)
    return str(p)
