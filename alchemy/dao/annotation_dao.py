from typing import List

from alchemy.data.request.annotation_request import AnnotationUpsertRequest
from alchemy.db.model import ClassificationAnnotation, EntityTypeEnum


# TODO we should move this to a utility class in case we have more than one
#  entity_type in the future
def _construct_context(entity_type, entity):
    if entity_type == EntityTypeEnum.COMPANY:
        context = {"text": "N/A", "meta": {"name": entity, "domain": entity}}
    else:
        context = {
            "text": "N/A",
            "meta": {
                "name": entity
                # TODO we probably should name `domain` to
                #  something else according to the entity type
            },
        }
    return context


class AnnotationDao:
    def __init__(self, db):
        self.db = db

    def upsert_annotation(self, upsert_request: AnnotationUpsertRequest):
        annotation = self._upsert_annotation_helper(upsert_request)

        self.db.session.add(annotation)
        self._commit_to_db()

    def upsert_annotations_bulk(self, upsert_requests: List[AnnotationUpsertRequest]):
        annotations = [
            self._upsert_annotation_helper(create_request)
            for create_request in upsert_requests
        ]
        self.db.session.add_all(annotations)
        self._commit_to_db()

    def _check_existing_annotation(self, entity_type, entity, label, user_id):
        annotation = (
            self.db.session.query(ClassificationAnnotation)
            .filter_by(
                entity_type=entity_type, entity=entity, user_id=user_id, label=label
            )
            .one_or_none()
        )
        return annotation

    def _upsert_annotation_helper(self, upsert_request: AnnotationUpsertRequest):
        annotation = self._check_existing_annotation(
            upsert_request.entity_type,
            upsert_request.entity,
            upsert_request.label,
            upsert_request.user_id,
        )
        if annotation is None:
            context = _construct_context(
                upsert_request.entity_type, upsert_request.entity
            )
            annotation = ClassificationAnnotation(
                entity_type=upsert_request.entity_type,
                entity=upsert_request.entity,
                user_id=upsert_request.user_id,
                label=upsert_request.label,
                value=upsert_request.value,
                context=context,
            )
        else:
            annotation.value = upsert_request.value
        return annotation

    def _commit_to_db(self):
        try:
            self.db.session.commit()
        except Exception:
            self.db.session.rollback()
            raise
