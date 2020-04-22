import json
import logging
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
from db.model import (
    BackgroundJob, Task, AnnotationRequest,
    AnnotationRequestStatus, AnnotationType,
)
from db.task import Task as _Task
from db.model import Database, User, EntityType, Context, Entity, Label, \
    ClassificationAnnotation, EntityTypeEnum, get_or_create
from shared.utils import generate_md5_hash

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
    logging.root.setLevel(logging.INFO)
    logging.info(f"Migrate json to db")

    import argparse

    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--tasks_dir', default="/annotation_tool/__tasks")
    args = parser.parse_args()

    tasks_dir = args.tasks_dir

    logging.info(f"Tasks dir: {tasks_dir}")
    environ['ANNOTATION_TOOL_TASKS_DIR'] = tasks_dir

    # # Uncomment to drop database and restart from scratch.
    # from db.model import Base
    # Base.metadata.drop_all(db.engine)
    # Base.metadata.create_all(db.engine)

    # TODO run this on all tasks.
    task_ids = ["8a79a035-56fa-415c-8202-9297652dfe75"]
    for task_id in task_ids:
        task = convert_task(task_id)

        usernames = _get_all_annotators_from_requested(task_id)
        for username in usernames:
            convert_annotation_request_in_batch(
                task_id=task_id,
                username=username
            )

        usernames = _get_all_annotators_from_annotated(task_id)
        for username in usernames:
            convert_annotation_result_in_batch(
                task_id=task_id,
                username=username,
            )

    print("--Counts--")
    tables = [Label, EntityType, Entity, User, Context,
              ClassificationAnnotation, BackgroundJob, Task, AnnotationRequest]
    for t in tables:
        print(t, db.session.query(t).count())
