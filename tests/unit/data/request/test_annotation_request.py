from dataclasses import asdict, fields

from alchemy.data.request.annotation_request import AnnotationUpsertRequest
from alchemy.db.model import EntityTypeEnum, AnnotationValue


def test_build_annotation_upsert_request_with_valid_data():
    dict_data = {
        "entity_type": EntityTypeEnum.COMPANY,
        "entity": "google.com",
        "label": "B2C",
        "user_id": 1,
        "value": AnnotationValue.POSITIVE,
    }

    upsert_request = AnnotationUpsertRequest.from_dict(dict_data=dict_data)

    assert asdict(upsert_request) == dict_data
    assert bool(upsert_request) is True


def test_build_annotation_upsert_request_with_invalid_empty_data():
    dict_data = {}

    upsert_request = AnnotationUpsertRequest.from_dict(dict_data=dict_data)

    assert upsert_request.has_errors()

    assert len(upsert_request.errors) == len(fields(AnnotationUpsertRequest))

    for field in fields(AnnotationUpsertRequest):
        assert {
            "parameter": "dict_data",
            "message": f"Missing field {field.name}.",
        } in upsert_request.errors

    assert bool(upsert_request) is False


def test_build_annotation_upsert_request_with_invalid_wrong_type_data():
    dict_data = {
        "entity_type": 123,
        "entity": 3.15,
        "label": 23,
        "user_id": "125",
        "value": "-1",
    }

    upsert_request = AnnotationUpsertRequest.from_dict(dict_data=dict_data)

    assert upsert_request.has_errors()

    assert len(upsert_request.errors) == len(fields(AnnotationUpsertRequest))

    for field in fields(AnnotationUpsertRequest):
        assert {
            "parameter": "dict_data",
            "message": f"Field {field.name} expects {field.type} "
            f"but received {type(dict_data[field.name])}",
        } in upsert_request.errors

    assert bool(upsert_request) is False
