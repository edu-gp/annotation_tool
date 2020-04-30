import time
import os
import shutil

from .no_deps.paths import (
    _get_config_fname, _get_exported_data_fname
)
from .no_deps.utils import get_env_int, get_env_bool

from db.model import (
    Task, TextClassificationModel,
    Label, ClassificationTrainingData,
    EntityType, EntityTypeEnum,
    get_or_create
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


def prepare_task_for_training(dbsession, task_id) -> TextClassificationModel:
    """Exports the model and save config when the model is training it does
    not need access to the Task object.

    Returns the directory in which all the prepared info are stored.
    """
    task = dbsession.query(Task).filter_by(id=task_id).one_or_none()

    # Model uuid by default is the task's uuid; One model per task.
    uuid = task.get_uuid()
    version = TextClassificationModel.get_next_version(dbsession, uuid)

    # By default use the first label the task has.
    label_name = task.get_labels()[0]
    # By default use the Company entity type
    # TODO this should be part of Task
    entity_type = get_or_create(
        dbsession, EntityType, name=EntityTypeEnum.COMPANY)

    label = dbsession.query(Label).filter_by(
        name=label_name, entity_type=entity_type).first()

    # TODO these defaults should be stored in Task
    jsonl_file_path = task.get_data_filenames(abs=True)[0]
    entity_text_lookup_fn = get_entity_text_lookup_function(
        jsonl_file_path, 'meta.domain', 'text', entity_type.id
    )

    data = ClassificationTrainingData.create_for_label(
        dbsession, label, entity_text_lookup_fn)

    config = generate_config()

    model = TextClassificationModel(uuid=uuid, version=version, task=task,
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
    shutil.copyfile(data.path(abs=True), _get_exported_data_fname(model_dir))

    return model
