from dataclasses import asdict, fields

from alchemy.data.request.annotation_request import AnnotationCreateRequest
from alchemy.db.model import EntityTypeEnum, AnnotationValue


def test_build_annotation_create_request_with_valid_data():
    dict_data = {
        "entity_type": EntityTypeEnum.COMPANY,
        "entity": "google.com",
        "label": "B2C",
        "user_id": 1,
        "value": AnnotationValue.POSITIVE,
    }

    create_request = AnnotationCreateRequest.from_dict(dict_data=dict_data)

    assert asdict(create_request) == dict_data
    assert bool(create_request) is True


def test_build_annotation_create_request_with_invalid_empty_data():
    dict_data = {}

    create_request = AnnotationCreateRequest.from_dict(dict_data=dict_data)

    assert create_request.has_errors()

    assert len(create_request.errors) == len(fields(AnnotationCreateRequest))

    for field in fields(AnnotationCreateRequest):
        assert {
            "parameter": "dict_data",
            "message": f"Missing field {field.name}.",
        } in create_request.errors

    assert bool(create_request) is False


def test_build_annotation_create_request_with_invalid_wrong_type_data():
    dict_data = {
        "entity_type": 123,
        "entity": 3.15,
        "label": 23,
        "user_id": "125",
        "value": "-1",
    }

    create_request = AnnotationCreateRequest.from_dict(dict_data=dict_data)

    assert create_request.has_errors()

    assert len(create_request.errors) == len(fields(AnnotationCreateRequest))

    for field in fields(AnnotationCreateRequest):
        assert {
            "parameter": "dict_data",
            "message": f"Field {field.name} expects {field.type} "
            f"but received {type(dict_data[field.name])}",
        } in create_request.errors

    assert bool(create_request) is False
