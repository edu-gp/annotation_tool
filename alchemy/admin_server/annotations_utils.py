from typing import List

from alchemy.db.model import AnnotationValue


def _parse_list(form: dict, key: str) -> List:
    value = form.get(key)
    result = []
    if value and isinstance(value, str):
        result = [x.strip() for x in value.splitlines()]
        result = [x for x in result if x]
    return result


def parse_form(form: dict):
    user = form.get("user")
    label = form.get("label")

    is_golden = form.get("is_golden", None)
    is_golden = True if is_golden else False

    entity_type = form.get("entity_type")
    if entity_type == "None":
        entity_type = None

    entities = _parse_list(form, "entities")
    annotations = _parse_list(form, "annotations")

    assert user, "User is required"
    assert label, "Label is required"
    assert entity_type, "Entity type is required"

    assert len(entities) == len(annotations), (
        f"Number of entities ({len(entities)}) does not match "
        f"with number of annotations ({len(annotations)})"
    )

    # For now, we leave it up to the user to make sure no duplicates.
    assert len(set(entities)) == len(entities), "There are duplicates in entities"

    acceptable_annotations = {
        AnnotationValue.POSITIVE,
        AnnotationValue.NEGTIVE,
        AnnotationValue.UNSURE,
    }

    for i in range(len(annotations)):
        annotations[i] = int(annotations[i])
        assert annotations[i] in acceptable_annotations, (
            f"Annotation {annotations[i]} is not in the list of "
            f"acceptable annotations {acceptable_annotations}"
        )

    return user, label, entities, annotations, entity_type, is_golden


def parse_bulk_upload_v2_form(form: dict):
    user = form.get("user")
    entity_type = form.get("entity_type")
    if entity_type == "None":
        entity_type = None

    value = form.get("value")

    entities = _parse_list(form, "entities")
    labels = _parse_list(form, "labels")

    assert user, "User is required"
    assert entity_type, "Entity type is required"
    assert value, "Annotation value is required"

    value = int(value)

    acceptable_annotations = {
        AnnotationValue.POSITIVE,
        AnnotationValue.NEGTIVE,
        AnnotationValue.UNSURE,
    }
    assert value in acceptable_annotations, (
        f"Value {value} is not in the list of "
        f"acceptable annotations {acceptable_annotations}"
    )
    assert len(entities) == len(labels), (
        f"Number of entities ({len(entities)}) does not match "
        f"with number of labels ({len(labels)})"
    )

    return user, entities, labels, entity_type, value
