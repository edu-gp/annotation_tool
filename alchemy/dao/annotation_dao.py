from typing import List

from alchemy.data.request.annotation_request import AnnotationCreateRequest
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

    def create_annotation(self, create_request: AnnotationCreateRequest):
        annotation = self._create_annotation_helper(create_request)

        self.db.session.add(annotation)
        self._commit_to_db()

    def create_annotations_bulk(self, create_requests: List[AnnotationCreateRequest]):
        annotations = [
            self._create_annotation_helper(create_request)
            for create_request in create_requests
        ]
        self.db.session.add_all(annotations)
        self._commit_to_db()

    def _find_existing_annotation(self, entity_type, entity, label, user_id):
        annotation = (
            self.db.session.query(ClassificationAnnotation)
            .filter_by(
                entity_type=entity_type, entity=entity, user_id=user_id, label=label
            )
            .one_or_none()
        )
        return annotation

    def _create_annotation_helper(self, create_request: AnnotationCreateRequest):
        annotation = self._find_existing_annotation(
            create_request.entity_type,
            create_request.entity,
            create_request.label,
            create_request.user_id,
        )
        if annotation is None:
            context = _construct_context(
                create_request.entity_type, create_request.entity
            )
            annotation = ClassificationAnnotation(
                entity_type=create_request.entity_type,
                entity=create_request.entity,
                user_id=create_request.user_id,
                label=create_request.label,
                value=create_request.value,
                context=context,
            )
        else:
            annotation.value = create_request.value
        return annotation

    def _commit_to_db(self):
        try:
            self.db.session.commit()
        except Exception:
            self.db.session.rollback()
            raise
