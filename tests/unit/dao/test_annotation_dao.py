from mockito import when
from sqlalchemy.exc import DatabaseError

from alchemy.dao.annotation_dao import AnnotationDao
from alchemy.data.request.annotation_request import AnnotationUpsertRequest
from alchemy.db.model import EntityTypeEnum, ClassificationAnnotation

entity_type = EntityTypeEnum.COMPANY
entity1 = "123.com"
entity2 = "234.com"
user_id = 1
label = "hotdog"
value = 1
context1 = {"text": "N/A", "meta": {"name": entity1, "domain": entity1}}
context2 = {"text": "N/A", "meta": {"name": entity2, "domain": entity2}}


def _populate_data(dbsession):
    annotation = ClassificationAnnotation(
        entity_type=entity_type,
        entity=entity1,
        user_id=user_id,
        label=label,
        value=value,
        context=context1,
    )
    dbsession.add(annotation)
    dbsession.commit()
    return annotation


def _count_annotations(dbsession):
    return dbsession.query(ClassificationAnnotation).count()


def test_upsert_annotation_new_instance(dbsession):
    annotation_dao = AnnotationDao(dbsession=dbsession)
    upsert_request = AnnotationUpsertRequest(
        entity_type=entity_type,
        entity=entity1,
        user_id=user_id,
        label=label,
        value=value,
        context=context1,
    )

    annotation_dao.upsert_annotation(upsert_request=upsert_request)

    num_of_annotations = _count_annotations(dbsession)
    assert num_of_annotations == 1

    saved_instance = (
        dbsession.query(ClassificationAnnotation)
        .filter_by(
            entity_type=entity_type, entity=entity1, user_id=user_id, label=label
        )
        .one_or_none()
    )

    assert saved_instance is not None
    assert saved_instance.entity_type == entity_type
    assert saved_instance.entity == entity1
    assert saved_instance.user_id == user_id
    assert saved_instance.label == label
    assert saved_instance.value == value
    assert saved_instance.context == context1


def test_upsert_annotation_update_instance(dbsession):
    existing_annotation = _populate_data(dbsession)

    annotation_dao = AnnotationDao(dbsession=dbsession)
    upsert_request = AnnotationUpsertRequest(
        entity_type=entity_type,
        entity=entity1,
        user_id=user_id,
        label=label,
        value=value * -1,
        context=context1,
    )

    annotation_dao.upsert_annotation(upsert_request=upsert_request)

    num_of_annotations = _count_annotations(dbsession)
    assert num_of_annotations == 1

    saved_instance = (
        dbsession.query(ClassificationAnnotation)
        .filter_by(
            entity_type=entity_type, entity=entity1, user_id=user_id, label=label
        )
        .one_or_none()
    )

    assert saved_instance is not None
    assert saved_instance.id == existing_annotation.id
    assert saved_instance.entity_type == entity_type
    assert saved_instance.entity == entity1
    assert saved_instance.user_id == user_id
    assert saved_instance.label == label
    assert saved_instance.value == value * -1
    assert saved_instance.context == context1


def test_upsert_annotations_bulk(dbsession):
    exisiting_annotation = _populate_data(dbsession)

    annotation_dao = AnnotationDao(dbsession=dbsession)
    upsert_request1 = AnnotationUpsertRequest(
        entity_type=entity_type,
        entity=entity1,
        user_id=user_id,
        label=label,
        value=value * -1,
        context=context1,
    )

    upsert_request2 = AnnotationUpsertRequest(
        entity_type=entity_type,
        entity=entity2,
        user_id=user_id,
        label=label,
        value=value,
        context=context2,
    )

    annotation_dao.upsert_annotations_bulk(
        upsert_requests=[upsert_request1, upsert_request2]
    )

    num_of_annotations = _count_annotations(dbsession)
    assert num_of_annotations == 2

    saved_instance1 = (
        dbsession.query(ClassificationAnnotation)
        .filter_by(
            entity_type=entity_type, entity=entity1, user_id=user_id, label=label
        )
        .one_or_none()
    )

    assert saved_instance1 is not None
    assert saved_instance1.id == exisiting_annotation.id
    assert saved_instance1.entity_type == entity_type
    assert saved_instance1.entity == entity1
    assert saved_instance1.user_id == user_id
    assert saved_instance1.label == label
    assert saved_instance1.value == value * -1
    assert saved_instance1.context == context1

    saved_instance2 = (
        dbsession.query(ClassificationAnnotation)
        .filter_by(
            entity_type=entity_type, entity=entity2, user_id=user_id, label=label
        )
        .one_or_none()
    )

    assert saved_instance2 is not None
    assert saved_instance2.entity_type == entity_type
    assert saved_instance2.entity == entity2
    assert saved_instance2.user_id == user_id
    assert saved_instance2.label == label
    assert saved_instance2.value == value
    assert saved_instance2.context == context2


def _prepare_for_retry_testcases(dbsession):
    annotation_dao = AnnotationDao(dbsession=dbsession)
    upsert_request = AnnotationUpsertRequest(
        entity_type=entity_type,
        entity=entity1,
        user_id=user_id,
        label=label,
        value=1,
        context=context1,
    )
    error_msg = "Mocked Exception!"
    database_error = DatabaseError(
        statement="Mocked statement", params={}, orig=error_msg
    )

    return annotation_dao, upsert_request, database_error


def test_upsert_annotation_failure_retry_success(dbsession):
    annotation_dao, upsert_request, database_error = _prepare_for_retry_testcases(
        dbsession
    )

    when(dbsession).commit().thenRaise(database_error).thenRaise(
        database_error
    ).thenReturn(True)

    annotation_dao.upsert_annotation(upsert_request=upsert_request)

    num_of_annotations = _count_annotations(dbsession)
    assert num_of_annotations == 1


def test_upsert_annotation_failure_retry_exceeded(dbsession):
    annotation_dao, upsert_request, database_error = _prepare_for_retry_testcases(
        dbsession
    )

    when(dbsession).commit().thenRaise(database_error).thenRaise(
        database_error
    ).thenRaise(database_error)
    try:
        annotation_dao.upsert_annotation(upsert_request=upsert_request)
    except DatabaseError:
        pass

    num_of_annotations = _count_annotations(dbsession)
    assert num_of_annotations == 0


def test_upsert_annotation_failure_non_retriable_error(dbsession, monkeypatch):
    annotation_dao, upsert_request, _ = _prepare_for_retry_testcases(dbsession)

    error_msg = "Mocked Exception!"

    def mock_commit():
        raise Exception(error_msg)

    monkeypatch.setattr(dbsession, "commit", mock_commit)

    try:
        annotation_dao.upsert_annotation(upsert_request=upsert_request)
    except Exception as e:
        assert str(e) == error_msg

    num_of_annotations = _count_annotations(dbsession)
    assert num_of_annotations == 0
