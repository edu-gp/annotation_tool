import copy
import hashlib
import json
from os import environ

from ar import fetch_all_ar_ids
from ar.data import fetch_annotation, _get_all_annotators_from_annotated, \
    get_or_create
from db.config import DevelopmentConfig
from db.model import Database, User, EntityType, Context, Entity, Label, \
    ClassificationAnnotation, EntityTypeEnum
from shared.utils import generate_md5_hash

ENTITY_TYPE_NAME = EntityTypeEnum.COMPANY
db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)


def convert_annotation_result_in_batch(task_id, username):
    annotation_request_ids = fetch_all_ar_ids(task_id, username)
    for ar_id in annotation_request_ids:
        anno = fetch_annotation(task_id, username, ar_id)
        _convert_single_annotation(anno, username)
        print("Converted annotation with ar_id {}".format(ar_id))


def _convert_single_annotation(anno, username):
    user = get_or_create(db.session, User, username=username)

    entity_type = get_or_create(db.session, EntityType, name=ENTITY_TYPE_NAME)

    annotation_context = json.dumps(anno["req"]["data"], sort_keys=True)
    context = get_or_create(db.session, Context, exclude_keys_in_retrieve=["data"],
                            hash=generate_md5_hash(annotation_context),
                            data=annotation_context)

    company_name = anno["req"]["data"]["meta"]["name"]
    company_name = company_name if company_name is not None else "unknown"
    domain = anno["req"]["data"]["meta"].get("domain", company_name)
    domain = domain if domain is not None else company_name
    entity = get_or_create(db.session, Entity, name=domain,
                           entity_type_id=entity_type.id)

    for label_name, label_value in anno["anno"]["labels"].items():
        label = get_or_create(db.session, Label, name=label_name,
                              entity_type_id=entity_type.id)

        _ = get_or_create(db.session, ClassificationAnnotation,
                          value=label_value,
                          entity_id=entity.id,
                          label_id=label.id,
                          user_id=user.id,
                          context_id=context.id)


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
