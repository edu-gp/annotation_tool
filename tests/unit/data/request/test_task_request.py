from dataclasses import asdict, fields

import pytest

from alchemy.data.request.task_request import TaskCreateRequest, TaskUpdateRequest
from alchemy.db.model import EntityTypeEnum


def test_build_task_create_request_with_valid_data():
    dict_data = {
        "name": "testhotdog",
        "entity_type": EntityTypeEnum.COMPANY,
        "labels": ["B2C"],
        "annotators": ["user1", "user2"],
        "data_files": ["data_file.txt"],
    }

    create_request = TaskCreateRequest.from_dict(dict_data=dict_data)

    assert asdict(create_request) == dict_data
    assert bool(create_request) is True


def test_build_task_create_request_with_invalid_empty_data():
    dict_data = {}

    create_request = TaskCreateRequest.from_dict(dict_data=dict_data)

    assert create_request.has_errors()

    assert len(create_request.errors) == len(fields(TaskCreateRequest))

    for field in fields(TaskCreateRequest):
        assert {
            "parameter": "dict_data",
            "message": f"Missing field {field.name}.",
        } in create_request.errors

    assert bool(create_request) is False


def test_build_task_create_request_with_invalid_wrong_type_data():
    dict_data = {
        "name": 123,
        "entity_type": 321,
        "labels": "B2C",
        "annotators": "123",
        "data_files": "234",
    }

    create_request = TaskCreateRequest.from_dict(dict_data=dict_data)

    assert create_request.has_errors()

    assert len(create_request.errors) == len(fields(TaskCreateRequest))

    for field in fields(TaskCreateRequest):
        assert {
            "parameter": "dict_data",
            "message": f"Field {field.name} expects {field.type} "
            f"but received {type(dict_data[field.name])}",
        } in create_request.errors

    assert bool(create_request) is False


@pytest.mark.parametrize("list_data,error_type", [([], "empty")])
def test_build_task_create_request_with_invalid_empty_list(list_data, error_type):
    dict_data = {
        "name": "testhotdog",
        "entity_type": EntityTypeEnum.COMPANY,
        "labels": list_data,
        "annotators": list_data,
        "data_files": list_data,
    }

    create_request = TaskCreateRequest.from_dict(dict_data=dict_data)

    assert create_request.has_errors()

    for field_name in ["annotators", "labels", "data_files"]:
        assert {
            "parameter": "dict_data",
            "message": f"Field {field_name} is {error_type}.",
        } in create_request.errors

    assert bool(create_request) is False
