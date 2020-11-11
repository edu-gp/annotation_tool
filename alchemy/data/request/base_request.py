from dataclasses import Field
from typing import List, Dict


class Request:
    pass


class InvalidRequest(Request):
    def __init__(self):
        self.errors = []

    def add_error(self, parameter, message):
        self.errors.append({"parameter": parameter, "message": message})

    def has_errors(self):
        return len(self.errors) > 0

    def __bool__(self):
        return False


class ValidRequest(Request):
    @classmethod
    def from_dict(cls, dict_data):
        raise NotImplementedError

    def __bool__(self):
        return True


def validate_request_data_common(
    fields: List[Field], dict_data: Dict, invalid_req: InvalidRequest
):
    for field in fields:
        if field.name not in dict_data:
            invalid_req.add_error(
                parameter="dict_data", message=f"Missing field {field.name}."
            )
        elif not isinstance(dict_data[field.name], field.type):
            invalid_req.add_error(
                parameter="dict_data",
                message=f"Field {field.name} expects {field.type} "
                f"but received {type(dict_data[field.name])}",
            )

    check_invalid_request_fields(fields, dict_data, invalid_req)


def check_invalid_request_fields(
    fields: List[Field], dict_data: Dict, invalid_req: InvalidRequest
):
    field_names = set([field.name for field in fields])
    for key in dict_data:
        if key not in field_names:
            invalid_req.add_error(
                parameter="dict_data", message=f"Invalid field {key}."
            )
