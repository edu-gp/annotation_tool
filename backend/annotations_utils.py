from typing import List
from db.model import AnnotationValue


def _parse_list(form: dict, key: str) -> List:
    value = form.get(key)
    result = []
    if value and isinstance(value, str):
        result = [x.strip() for x in value.splitlines()]
        result = [x for x in result if x]
    return result


def parse_form(form: dict):
    user = form.get('user')
    label = form.get('label')
    domains = _parse_list(form, 'domains')
    annotations = _parse_list(form, 'annotations')

    assert user, 'User is required'
    assert label, 'Label is required'

    assert len(domains) == len(annotations), \
        f'Number of domains ({len(domains)}) does not match ' \
        f'with number of annotations ({len(annotations)})'

    # For now, we leave it up to the user to make sure no duplicates.
    assert len(set(domains)) == len(domains), \
        "There are duplicates in domains"

    acceptable_annotations = set([
        AnnotationValue.POSITIVE,
        AnnotationValue.NEGTIVE,
        AnnotationValue.UNSURE,
    ])

    for i in range(len(annotations)):
        annotations[i] = int(annotations[i])
        assert annotations[i] in acceptable_annotations, \
            f"Annotation {annotations[i]} is not in the list of " \
            f"acceptable annotations {acceptable_annotations}"

    return user, label, domains, annotations
