from dataclasses import dataclass, fields

from alchemy.data.request.base_request import (
    ValidRequest,
    InvalidRequest,
    validate_request_data_common,
)


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
        data_fields = fields(cls)
        validate_request_data_common(
            fields=data_fields, dict_data=dict_data, invalid_req=invalid_req
        )
