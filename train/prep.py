import time
import os
import shutil
import hashlib

from .no_deps.paths import (
    _get_config_fname, _get_exported_data_fname
)
from .no_deps.utils import get_env_int, get_env_bool

from db.model import (
    TextClassificationModel, ClassificationTrainingData,
    EntityTypeEnum
)
from train.text_lookup import get_entity_text_lookup_function
from shared.utils import save_json


def generate_config():
    return {
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


def prepare_next_model_for_label(
        dbsession, label, raw_file_path,
        entity_type=EntityTypeEnum.COMPANY) -> TextClassificationModel:
    """Exports the model and save config when the model is training it does
    not need access to the Task object.

    Returns the directory in which all the prepared info are stored.
    """
    model_id = f"{os.environ.get('ALCHEMY_ENV', 'dev')}:{label}"
    model_id = hashlib.sha224(model_id.encode()).hexdigest()

    version = TextClassificationModel.get_next_version(dbsession, model_id)

    entity_text_lookup_fn = get_entity_text_lookup_function(
        raw_file_path, 'meta.domain', 'text', entity_type
    )

    data = ClassificationTrainingData.create_for_label(
        dbsession, entity_type, label, entity_text_lookup_fn)

    config = generate_config()

    model = TextClassificationModel(uuid=model_id, version=version,
                                    label=label,
                                    classification_training_data=data,
                                    config=config)
    dbsession.add(model)
    dbsession.commit()

    # Build up the model_dir
    model_dir = model.dir(abs=True)
    os.makedirs(model_dir, exist_ok=True)
    # Save config
    save_json(_get_config_fname(model_dir), config)
    # Copy over data
    print(data.path(abs=True))
    print(_get_exported_data_fname(model_dir))
    shutil.copyfile(data.path(abs=True), _get_exported_data_fname(model_dir))

    return model
