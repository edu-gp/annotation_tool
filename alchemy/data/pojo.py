from dataclasses import dataclass


@dataclass
class AnnotationCreationRequest:
    entity_type: str
    entity: str
    label: str
    user_id: int
    value: int = 0
