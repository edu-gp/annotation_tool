from dataclasses import asdict, fields

import pytest

from alchemy.data.request.task_request import TaskCreateRequest, TaskUpdateRequest
from alchemy.db.model import EntityTypeEnum


@pytest.mark.parametrize(
    "dict_data,request_class",
    [
        (
            {
                "name": "testhotdog",
                "entity_type": EntityTypeEnum.COMPANY,
                "labels": ["B2C"],
                "annotators": ["user1", "user2"],
                "data_files": ["data_file.txt"],
            },
            TaskCreateRequest,
        ),
        (
            {
                "id": 1,
                "name": "testhotdog",
                "entity_type": EntityTypeEnum.COMPANY,
                "labels": ["B2C"],
                "annotators": ["user1", "user2"],
                "data_files": ["data_file.txt"],
            },
            TaskUpdateRequest,
        ),
    ],
)
def test_build_task_request_with_valid_data(dict_data, request_class):

    request = request_class.from_dict(dict_data=dict_data)

    assert asdict(request) == dict_data
    assert bool(request) is True


@pytest.mark.parametrize(
    "dict_data,request_class", [({}, TaskCreateRequest), ({}, TaskUpdateRequest)]
)
def test_build_task_request_with_invalid_empty_data(dict_data, request_class):

    request = request_class.from_dict(dict_data=dict_data)

    assert request.has_errors()

    assert len(request.errors) == len(fields(request_class))

    for field in fields(request_class):
        assert {
            "parameter": "dict_data",
            "message": f"Missing field {field.name}.",
        } in request.errors

    assert bool(request) is False


@pytest.mark.parametrize(
    "dict_data,request_class",
    [
        (
            {
                "name": 123,
                "entity_type": 321,
                "labels": "B2C",
                "annotators": "123",
                "data_files": "234",
            },
            TaskCreateRequest,
        ),
        (
            {
                "id": "Sdf",
                "name": 123,
                "entity_type": 321,
                "labels": "B2C",
                "annotators": "123",
                "data_files": "234",
            },
            TaskUpdateRequest,
        ),
    ],
)
def test_build_task_request_with_invalid_wrong_type_data(dict_data, request_class):
    request = request_class.from_dict(dict_data=dict_data)

    assert request.has_errors()

    assert len(request.errors) == len(fields(request_class))

    for field in fields(request_class):
        assert {
            "parameter": "dict_data",
            "message": f"Field {field.name} expects {field.type} "
            f"but received {type(dict_data[field.name])}",
        } in request.errors

    assert bool(request) is False


@pytest.mark.parametrize(
    "dict_data,request_class",
    [
        (
            {
                "name": "testhotdog",
                "entity_type": EntityTypeEnum.COMPANY,
                "labels": [],
                "annotators": [],
                "data_files": [],
            },
            TaskCreateRequest,
        ),
        (
            {
                "id": 1,
                "name": "testhotdog",
                "entity_type": EntityTypeEnum.COMPANY,
                "labels": [],
                "annotators": [],
                "data_files": [],
            },
            TaskUpdateRequest,
        ),
    ],
)
def test_build_task_request_with_invalid_empty_list(dict_data, request_class):
    request = request_class.from_dict(dict_data=dict_data)

    assert request.has_errors()

    for field_name in ["annotators", "labels", "data_files"]:
        assert {
            "parameter": "dict_data",
            "message": f"Field {field_name} is empty.",
        } in request.errors

    assert bool(request) is False
