import copy
import hashlib
import json
import sys
from os import environ

sys.path.append(".")

from ar import fetch_all_ar_ids
from ar.data import fetch_annotation, _get_all_annotators_from_annotated
from db.config import DevelopmentConfig
from db.model import Database, User, EntityType, Context, Entity, Label, \
    ClassificationAnnotation

ENTITY_TYPE_NAME = "company"
db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)


def convert_annotation_result_in_batch(task_id, username):
    annotation_request_ids = fetch_all_ar_ids(task_id, username)
    for ar_id in annotation_request_ids:
        anno = fetch_annotation(task_id, username, ar_id)
        _convert_single_annotation(anno, username)
        print("Converted annotation with ar_id {}".format(ar_id))


def _convert_single_annotation(anno, username):
    user = get_or_create(User, username=username)

    entity_type = get_or_create(EntityType, name=ENTITY_TYPE_NAME)

    annotation_context = json.dumps(anno["req"]["data"], sort_keys=True)
    context = get_or_create(Context, exclude_keys_in_read=["data"],
                            hash=hashlib.md5(
                                annotation_context.encode()).hexdigest(),
                            data=annotation_context)

    company_name = anno["req"]["data"]["meta"]["name"]
    company_name = company_name if company_name is not None else "unknown"
    domain = anno["req"]["data"]["meta"].get("domain", company_name)
    domain = domain if domain is not None else company_name
    entity = get_or_create(Entity, name=domain,
                           entity_type_id=entity_type.id)

    for label_name, label_value in anno["anno"]["labels"].items():
        label = get_or_create(Label, name=label_name,
                              entity_type_id=entity_type.id)

        _ = get_or_create(ClassificationAnnotation,
                          value=label_value,
                          entity_id=entity.id,
                          label_id=label.id,
                          user_id=user.id,
                          context_id=context.id)


def get_or_create(model, exclude_keys_in_read=None, **kwargs):
    if exclude_keys_in_read is None:
        exclude_keys_in_read = []
    read_kwargs = copy.copy(kwargs)
    for key in exclude_keys_in_read:
        read_kwargs.pop(key, None)

    instance = db.session.query(model).filter_by(**read_kwargs).one_or_none()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        db.session.add(instance)
        db.session.commit()
        print("Created a new instance of {}".format(instance))
        return instance


if __name__ == "__main__":
    environ['ANNOTATION_TOOL_TASKS_DIR'] = "/annotation_tool/__tasks"

    task_ids = ["8a79a035-56fa-415c-8202-9297652dfe75"]
    for task_id in task_ids:
        usernames = _get_all_annotators_from_annotated(task_id)
        for username in usernames:
            convert_annotation_result_in_batch(
                task_id=task_id,
                username=username,
            )
