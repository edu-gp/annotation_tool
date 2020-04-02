import time

from ar.data import export_labeled_examples
from shared.utils import save_json
from db.task import Task

from .paths import (
    _get_latest_model_version, _get_version_dir
)
from .no_deps.paths import (
    _get_config_fname, _get_exported_data_fname
)

from .no_deps.utils import get_env_int, get_env_bool


def get_next_version(task_id):
    task = Task.fetch(task_id)
    version = _get_latest_model_version(task.task_id) + 1
    return version


def prepare_task_for_training(task_id, version):
    """Exports the model and save config when the model is training it does
    not need access to the Task object.

    Returns the directory in which all the prepared info are stored.
    """
    version_dir = _get_version_dir(task_id, version)
    print(f"Training model version={version} for task={task_id}")
    print(f"Storing results in {version_dir}")

    config_fname = _get_config_fname(version_dir)
    data_fname = _get_exported_data_fname(version_dir)

    # Save config
    config = {
        'created_at': time.time(),
        'test_size': 0.3,
        'random_state': 42,
        # TODO: Rename "train_config" to "model_config", or something more generic.
        # since train_config also includes config for inference...
        # NOTE: Env vars are used as global defaults. Eventually let user pass in
        # custom configs.
        'train_config': {
            'num_train_epochs': get_env_int("TRANSFORMER_TRAIN_EPOCHS", 5),
            'sliding_window': get_env_bool("TRANSFORMER_SLIDING_WINDOW", True),
            'max_seq_length': get_env_int("TRANSFORMER_MAX_SEQ_LENGTH", 512),
            'train_batch_size': get_env_int("TRANSFORMER_TRAIN_BATCH_SIZE", 8),
            # NOTE: Specifying a large batch size during inference makes the
            # process take up unnessesarily large amounts of memory.
            # We'll only toggle this on at inference time.
            # 'eval_batch_size': get_env_int("TRANSFORMER_EVAL_BATCH_SIZE", 8),
        }
    }
    save_json(config_fname, config)

    # Export labeled examples
    print("Export labeled examples...")
    export_labeled_examples(task_id, outfile=data_fname)

    return version_dir
