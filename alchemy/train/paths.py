from alchemy.db.fs import models_dir


def get_model_dir(uuid, version):
    # TODO: Legacy function with side effect of creating a dir
    p = models_dir(None, as_path=True) / uuid / str(version)
    p.mkdir(parents=True, exist_ok=True)
    print('model_dir =', p)
    return str(p)
