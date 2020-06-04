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
    EntityTypeEnum, AnnotationRequestStatus, AnnotationType,
    User, Task,
    AnnotationRequest, ClassificationAnnotation,
    ClassificationTrainingData, TextClassificationModel,
    AnnotationSource)
from db._task import _Task as _Task

db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)


def convert_annotation_result_in_batch(task_uuid, username):
    annotated_ar_ids = fetch_all_ar_ids(task_uuid, username)
    for ar_id in annotated_ar_ids:
        anno = fetch_annotation(task_uuid, username, ar_id)
        _convert_single_annotation(anno, username)
        print("Converted annotation with ar_id {}".format(ar_id))


def _make_company_entity(meta_dict, text=None):
    company_name = meta_dict.get("name")
    domain = meta_dict.get("domain", company_name)
    if domain is None:
        # To make sure we don't lose data, assign a unique UNK entity
        assert text
        domain = f"UNK_ENTITY_{hash(text)}"
    return EntityTypeEnum.COMPANY, domain


def _convert_single_annotation(anno, username):
    user = get_or_create(db.session, User, username=username)

    entity_type, entity = _make_company_entity(anno["req"]["data"]["meta"],
                                               text=anno["req"]["data"]["text"])

    if entity:
        # TODO WARNING: This only works for this migration where there is only
        #  one label in the annotation result.
        for label, label_value in anno["anno"]["labels"].items():
            annotation = get_or_create(db.session, ClassificationAnnotation,
                                       value=label_value,
                                       entity_type=entity_type,
                                       entity=entity,
                                       label=label,
                                       user_id=user.id,
                                       context=anno["req"]["data"],
                                       source=AnnotationSource.ALCHMEY,
                                       exclude_keys_in_retrieve=['context'],)
            return annotation.id


def convert_annotation_request_in_batch(task_uuid, task, username):
    requested_ar_ids = fetch_all_ar(task_uuid, username)

    label = task.get_labels()[0]

    for order, ar_id in enumerate(requested_ar_ids):
        req = fetch_ar(task_uuid, username, ar_id)
        _convert_single_request_with_annotated_result(
            req, username, task_uuid, task.id, order, label)
        print("Converted request with ar_id {}".format(ar_id))


def _convert_single_request_with_annotated_result(
        req, username, task_uuid, task_id, order, label):
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
    entity_type, entity = _make_company_entity(req["data"]["meta"],
                                               text=req['data']['text'])

    anno_from_file = fetch_annotation(task_uuid, username, ar_id=req['ar_id'])
    if anno_from_file:
        annotation_id = _convert_single_annotation(anno_from_file, username)
        logging.info("Converted annotation {} with ar_id {}".format(
            annotation_id, req['ar_id']))
    else:
        annotation_id = None
        logging.info("No annotation results found for this request {}".
                     format(req['ar_id']))

    if entity:
        get_or_create(
            db.session, AnnotationRequest,
            user_id=user.id,
            context=req,
            entity_type=entity_type,
            entity=entity,
            label=label,
            annotation_type=AnnotationType.ClassificationAnnotation,
            status=AnnotationRequestStatus.Pending,
            task_id=task_id,
            order=order,
            name=req['ar_id'],
            exclude_keys_in_retrieve=['context']
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
    os.system(f'cp -r {data_dir}/* {os.path.join(fsdir, RAW_DATA_DIR)}')

    # Training data and models are moved over below.

    # =========================================================================
    # Migrate data to database

    logging.info(f"Tasks dir: {tasks_dir}")
    environ['ANNOTATION_TOOL_TASKS_DIR'] = tasks_dir

    if drop:
        # Drop database and restart from scratch.
        # Note: If this gets stuck, make sure all your other sessions are
        # closed (e.g. restart all processes in supervisor)
        from db.model import Base
        Base.metadata.drop_all(db.engine)
        Base.metadata.create_all(db.engine)

    mock_time = 1000000000

    #task_uuids = ["fe9a1e62-80e3-4e58-9e12-3247ac0d18f5"]
    #task_uuids = ["f29dca1f-f03b-4667-b7fd-643e6f1a4611"]
    task_uuids = os.listdir(tasks_dir)

    from tqdm import tqdm
    for task_uuid in tqdm(task_uuids):
        # Check if this is a proper task.
        if not os.path.isfile(tasks_dir + '/' + task_uuid + '/config.json'):
            continue

        # Task
        _task = _Task.fetch(task_uuid)
        # Although names are not forced to be unique, I doubt any existing use case
        # has a duplicate name.
        task_default_params = _task.to_json()
        task_default_params['uuid'] = task_uuid
        task = get_or_create(
            db.session, Task,
            name=_task.name, default_params=task_default_params,
            exclude_keys_in_retrieve=['default_params']
        )

        # Annotation Requests (annotation results that are associated with
        # the requests will be created here so we can associate them together.)
        usernames = _get_all_annotators_from_requested(task_uuid)
        for username in usernames:
            convert_annotation_request_in_batch(
                task_uuid=task_uuid,
                task=task,
                username=username
            )

        # Annotations (There are overlaps with the annotation requests
        # migration. The rest of the annotations are not associated with
        # requests.)
        usernames = _get_all_annotators_from_annotated(task_uuid)
        for username in usernames:
            convert_annotation_result_in_batch(
                task_uuid=task_uuid,
                username=username,
            )

        # Models
        models_dir = os.path.join(tasks_dir, task_uuid, 'models')
        print("models_dir", models_dir)
        if os.path.isdir(models_dir):
            for model_version in os.listdir(models_dir):

                _source_dir = os.path.join(models_dir, model_version)

                # Training Data
                _source_data_path = os.path.join(_source_dir, 'data.jsonl')
                data = None
                if os.path.isfile(_source_data_path):
                    label = task.default_params['labels'][0]
                    data = get_or_create(
                        db.session, ClassificationTrainingData,
                        label=label,
                        created_at=datetime.datetime.fromtimestamp(mock_time))
                    mock_time += 1

                    _target_path = os.path.join(fsdir, data.path())
                    os.makedirs(os.path.dirname(_target_path), exist_ok=True)
                    os.system(f'cp {_source_data_path} {_target_path}')

                # Model
                _source_config_path = os.path.join(_source_dir, 'config.json')
                _config = {}
                if os.path.isfile(_source_config_path):
                    with open(_source_config_path) as f:
                        _config = json.loads(f.read())

                db_model = get_or_create(
                    db.session,
                    TextClassificationModel,
                    task_id=task.id,
                    uuid=task_uuid,
                    version=int(model_version),
                    config=_config,
                    classification_training_data=data,
                    exclude_keys_in_retrieve=['config']
                )

                # TODO why is there an extra inference folder under each model?

                _target_dir = os.path.join(fsdir, db_model.dir())
                os.system(f'mkdir -p {_target_dir}')
                os.system(f'cp -r {_source_dir}/* {_target_dir}')

                logging.info(f"Created Model {db_model}")

    print("--Counts--")
    tables = [
        User, Task,
        AnnotationRequest, ClassificationAnnotation,
        ClassificationTrainingData, TextClassificationModel]
    for t in tables:
        print(t, db.session.query(t).count())
