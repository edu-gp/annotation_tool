import json
import logging
import os
import datetime
from os import environ

# Filesystem based data:
from ar.data import (
    # Requests
    _get_all_annotators_from_requested,
    fetch_all_ar,
    fetch_ar,

    # Annotations
    _get_all_annotators_from_annotated,
    fetch_all_ar_ids,
    fetch_annotation,
)

from db.config import DevelopmentConfig
from db.fs import (
    filestore_base_dir, RAW_DATA_DIR, TRAINING_DATA_DIR, MODELS_DIR
)
from db.model import (
    Database, get_or_create,
    EntityType, EntityTypeEnum, AnnotationRequestStatus, AnnotationType,
    User, Context, Entity, Label, Task,
    AnnotationRequest, ClassificationAnnotation,
    ClassificationTrainingData, TextClassificationModel, FileInference,
)
from db.task import Task as _Task
from shared.utils import generate_md5_hash, stem

db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)


def _get_or_create_company_entity(company_name, domain):
    entity_type = get_or_create(
        db.session, EntityType, name=EntityTypeEnum.COMPANY)
    company_name = company_name if company_name is not None else "unknown"
    domain = domain if domain is not None else company_name
    entity = get_or_create(db.session, Entity, name=domain,
                           entity_type_id=entity_type.id)
    return entity_type, entity


def _get_or_create_anno_context(json_data):
    annotation_context = json.dumps(json_data, sort_keys=True)
    context = get_or_create(db.session, Context,
                            exclude_keys_in_retrieve=["data"],
                            hash=generate_md5_hash(annotation_context),
                            data=annotation_context)
    return context


def convert_annotation_result_in_batch(task_id, username):
    annotated_ar_ids = fetch_all_ar_ids(task_id, username)
    for ar_id in annotated_ar_ids:
        anno = fetch_annotation(task_id, username, ar_id)
        _convert_single_annotation(anno, username)
        print("Converted annotation with ar_id {}".format(ar_id))


def _convert_single_annotation(anno, username):
    user = get_or_create(db.session, User, username=username)

    company_name = anno["req"]["data"]["meta"]["name"]
    domain = anno["req"]["data"]["meta"].get("domain", company_name)
    entity_type, entity = _get_or_create_company_entity(company_name, domain)

    context = _get_or_create_anno_context(anno["req"]["data"])

    for label_name, label_value in anno["anno"]["labels"].items():
        label = get_or_create(db.session, Label, name=label_name,
                              entity_type_id=entity_type.id)

        _ = get_or_create(db.session, ClassificationAnnotation,
                          value=label_value,
                          entity_id=entity.id,
                          label_id=label.id,
                          user_id=user.id,
                          context_id=context.id)


def convert_annotation_request_in_batch(task_id, username):
    requested_ar_ids = fetch_all_ar(task_id, username)
    for idx, ar_id in enumerate(requested_ar_ids):
        req = fetch_ar(task_id, username, ar_id)
        _convert_single_request(req, username, task_id, order=idx)
        print("Converted request with ar_id {}".format(ar_id))


def _convert_single_request(req, username, task_id, order):
    user = get_or_create(db.session, User, username=username)

    '''
    Example:
    {
        "ar_id": "0c4a398f9d3ab6bae0dd40f8ce7092720131c08e516466f02231a3d3",
        "fname": "/Users/blah/myfile.jsonl",
        "line_number": 161,
        "score": 0.3397799678402079,
        "data": {
            "text": "Provider of an online learning pl...",
            "meta": {
                "name": "ACME",
                "domain": "acme.com"
            }
        }
    }
    '''
    company_name = req["data"]["meta"]["name"]
    domain = req["data"]["meta"].get("domain", company_name)
    entity_type, entity = _get_or_create_company_entity(company_name, domain)

    context = _get_or_create_anno_context(req["data"])

    get_or_create(
        db.session, AnnotationRequest,
        user_id=user.id,
        context_id=context.id,
        annotation_type=AnnotationType.ClassificationAnnotation,
        status=AnnotationRequestStatus.Pending,
        task_id=task_id,
        order=order,
        name=req['ar_id'],
        additional_info={
            'ar_id': req['ar_id'],
            'fname': req['fname'],
            'line_number': req['line_number'],
            'score': req['score']
        },
        source={
            'source': 'db-migration'
        },
        exclude_keys_in_retrieve=['additional_info', 'source']
    )


def convert_task(task_id):
    _task = _Task.fetch(task_id)
    # Although names are not forced to be unique, I doubt any existing use case
    # has a duplicate name.
    return get_or_create(
        db.session, Task,
        name=_task.name, default_params=_task.to_json(),
        exclude_keys_in_retrieve=['default_params']
    )


if __name__ == "__main__":
    """
    python -m scripts.migrate_json_to_db --tasks_dir __tasks --data_dir __data --drop
    """

    logging.root.setLevel(logging.INFO)
    logging.info(f"Migrate json to db")

    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--tasks_dir', default="/annotation_tool/__tasks")
    parser.add_argument('--data_dir', default="/annotation_tool/__data")
    parser.add_argument('--drop', default=False, action='store_true')
    args = parser.parse_args()

    tasks_dir = args.tasks_dir
    data_dir = args.data_dir
    drop = args.drop

    # =========================================================================
    # Migrate data to new filestore layout

    fsdir = filestore_base_dir()
    os.makedirs(fsdir, exist_ok=True)
    os.makedirs(os.path.join(fsdir, RAW_DATA_DIR), exist_ok=True)
    os.makedirs(os.path.join(fsdir, TRAINING_DATA_DIR), exist_ok=True)
    os.makedirs(os.path.join(fsdir, MODELS_DIR), exist_ok=True)

    # Move over raw data
    os.system(f'cp -r {data_dir}/ {os.path.join(fsdir, RAW_DATA_DIR)}')

    # Training data and models are moved over below.

    # =========================================================================
    # Migrate data to database

    logging.info(f"Tasks dir: {tasks_dir}")
    environ['ANNOTATION_TOOL_TASKS_DIR'] = tasks_dir

    if drop:
        # Drop database and restart from scratch.
        from db.model import Base
        Base.metadata.drop_all(db.engine)
        Base.metadata.create_all(db.engine)

    mock_time = 1000000000

    # task_ids = ["8a79a035-56fa-415c-8202-9297652dfe75"]
    task_ids = os.listdir(tasks_dir)

    for task_id in task_ids:
        # Check if this is a proper task.
        if not os.path.isfile(tasks_dir + '/' + task_id + '/config.json'):
            continue

        # Task
        task = convert_task(task_id)

        # Annotation Requests
        usernames = _get_all_annotators_from_requested(task_id)
        for username in usernames:
            convert_annotation_request_in_batch(
                task_id=task_id,
                username=username
            )

        # Annotations
        usernames = _get_all_annotators_from_annotated(task_id)
        for username in usernames:
            convert_annotation_result_in_batch(
                task_id=task_id,
                username=username,
            )

        # Models
        models_dir = os.path.join(tasks_dir, task_id, 'models')
        print("models_dir", models_dir)
        if os.path.isdir(models_dir):
            for model_version in os.listdir(models_dir):

                _source_dir = os.path.join(models_dir, model_version)

                # Training Data
                # If the data doesn't exist, this model wasn't set up properly.
                _data_path = f'{_source_dir}/data.jsonl'
                if not os.path.isfile(_data_path):
                    continue

                entity_type = get_or_create(
                    db.session, EntityType, name=EntityTypeEnum.COMPANY)
                label = get_or_create(
                    db.session, Label, name=task.default_params['labels'][0],
                    entity_type_id=entity_type.id)
                data = get_or_create(
                    db.session, ClassificationTrainingData, label_id=label.id,
                    created_at=datetime.datetime.fromtimestamp(mock_time))
                mock_time += 1

                _target_path = os.path.join(fsdir, data.path())
                os.makedirs(os.path.dirname(_target_path), exist_ok=True)
                os.system(f'cp {_data_path} {_target_path}')

                # Model & Inference
                # Only keep the models that have finished training.
                _config_path = f'{_source_dir}/config.json'
                _inference_dir = f'{_source_dir}/inference'
                if not (
                    os.path.isfile(_config_path) and
                    os.path.isdir(_inference_dir) and
                    len(os.listdir(_inference_dir)) > 0 and
                    os.path.isfile(f'{_source_dir}/metrics.json')
                ):
                    continue

                # If we get to here, then this model was properly trained.
                # Migrate over the model and inference.

                with open(_config_path) as f:
                    _config = json.loads(f.read())
                model_data = {
                    'training_data': data.path(),
                    'config': _config
                }

                db_model = get_or_create(
                    db.session,
                    TextClassificationModel,
                    uuid=task_id,
                    version=int(model_version),
                    data=model_data,
                    exclude_keys_in_retrieve=['data']
                )

                _target_dir = os.path.join(fsdir, db_model.dir())
                os.system(f'mkdir -p {_target_dir}')
                os.system(f'cp -r {_source_dir}/ {_target_dir}')

                logging.info("Created Model "
                             f"data={model_data}")

                # Inference
                for file in os.listdir(os.path.join(_source_dir, 'inference')):
                    if file.endswith('.npy'):
                        inf = get_or_create(
                            db.session, FileInference, model_id=db_model.id,
                            input_filename=f'{stem(file)}.jsonl')
                        logging.info("Created FileInference "
                                     f"input_filename={inf.input_filename} "
                                     f"path={inf.path()}")

    print("--Counts--")
    tables = [
        User, Context, Entity, Label, Task,
        AnnotationRequest, ClassificationAnnotation,
        ClassificationTrainingData, TextClassificationModel, FileInference]
    for t in tables:
        print(t, db.session.query(t).count())
