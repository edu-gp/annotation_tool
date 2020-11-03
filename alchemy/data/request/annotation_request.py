from dataclasses import dataclass, fields, asdict

from alchemy.data.request.base_request import ValidRequest, InvalidRequest


@dataclass
class AnnotationUpsertRequest(ValidRequest):
    entity_type: str
    entity: str
    label: str
    user_id: int
    value: int
    context: dict

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

        field_names = set([field.name for field in fields(cls)])
        for key in dict_data:
            if key not in field_names:
                invalid_req.add_error(
                    parameter="dict_data", message=f"Invalid field {key}."
                )
