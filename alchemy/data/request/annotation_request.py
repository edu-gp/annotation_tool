from dataclasses import dataclass, fields

from alchemy.data.request.base_request import ValidRequest, InvalidRequest


@dataclass
class AnnotationCreateRequest(ValidRequest):
    entity_type: str
    entity: str
    label: str
    user_id: int
    value: int

    @classmethod
    def from_dict(cls, dict_data):
        invalid_req = InvalidRequest()

        cls._validate_request_data(dict_data, invalid_req)

        if invalid_req.has_errors():
            return invalid_req

        return cls(**dict_data)

    @classmethod
    def _validate_request_data(cls, dict_data, invalid_req):
        for field in fields(cls):
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
