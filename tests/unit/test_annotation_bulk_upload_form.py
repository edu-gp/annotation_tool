import pytest

from alchemy.admin_server.annotations import _upsert_annotation
from alchemy.admin_server.annotations_utils import (
    _parse_list,
    parse_bulk_upload_v2_form,
    parse_form,
)
from alchemy.db.model import ClassificationAnnotation


def test__parse_list():
    # No such key
    assert len(_parse_list({}, "blah")) == 0

    # Invalid input
    assert len(_parse_list({"x": ["a", "b"]}, "x")) == 0

    # Valid input
    assert _parse_list({"x": "a\nb"}, "x") == ["a", "b"]
    assert _parse_list({"x": "a\r\n  b"}, "x") == ["a", "b"]


def test_parse_form():
    form = {}
    with pytest.raises(Exception, match="User is required"):
        parse_form(form)

    form = {"user": "a"}
    with pytest.raises(Exception, match="Label is required"):
        parse_form(form)

    form = {"user": "a", "label": "b"}
    with pytest.raises(Exception, match="Entity type is required"):
        parse_form(form)

    form = {"user": "a", "label": "b", "entity_type": "company"}
    user, label, domains, annotations, entity_type, is_golden = parse_form(form)
    assert user == "a"
    assert label == "b"
    assert len(domains) == 0
    assert len(annotations) == 0
    assert entity_type == "company"
    assert is_golden is False

    form = {"user": "a", "label": "b", "entities": "a.com", "entity_type": "company"}
    with pytest.raises(
        Exception,
        match=r"Number of entities .* does not match .* number of annotations .*",
    ):
        parse_form(form)

    form = {
        "user": "a",
        "label": "b",
        "entities": "a.com",
        "annotations": "1\n-1",
        "entity_type": "company",
    }
    with pytest.raises(
        Exception,
        match=r"Number of entities .* does not match .* number of annotations .*",
    ):
        parse_form(form)

    form = {
        "user": "a",
        "label": "b",
        "entities": "a.com\nb.com",
        "annotations": "1\n-1",
        "entity_type": "company",
        "is_golden": "true",
    }
    user, label, domains, annotations, entity_type, is_golden = parse_form(form)
    assert user == "a"
    assert label == "b"
    assert len(domains) == 2
    assert len(annotations) == 2
    assert entity_type == "company"
    assert is_golden is True

    form = {
        "user": "a",
        "label": "b",
        "entities": "a.com\nb.com",
        "annotations": "1\n5",
        "entity_type": "company",
    }
    with pytest.raises(
        Exception, match=r"Annotation 5 is not in the list of acceptable annotations .*"
    ):
        parse_form(form)


def test_parse_bulk_upload_v2_form():
    form = {}
    with pytest.raises(Exception, match="User is required"):
        parse_bulk_upload_v2_form(form)

    form = {"user": "a"}
    with pytest.raises(Exception, match="Entity type is required"):
        parse_bulk_upload_v2_form(form)

    form = {"user": "a", "entity_type": "company"}
    with pytest.raises(Exception, match="Annotation value is required"):
        parse_bulk_upload_v2_form(form)

    form = {"user": "a", "entity_type": "company", "value": "1"}
    user, entities, labels, entity_type, value = parse_bulk_upload_v2_form(form)
    assert user == "a"
    assert value == 1
    assert len(labels) == 0
    assert len(entities) == 0
    assert entity_type == "company"

    form = {"user": "a", "value": "1", "entities": "a.com", "entity_type": "company"}
    with pytest.raises(
        Exception, match=r"Number of entities .* does not match .* number of labels .*"
    ):
        parse_bulk_upload_v2_form(form)

    form = {
        "user": "a",
        "value": "1",
        "entities": "a.com",
        "labels": "b2c\nhotdog",
        "entity_type": "company",
    }
    with pytest.raises(
        Exception, match=r"Number of entities .* does not match .* number of labels .*"
    ):
        parse_bulk_upload_v2_form(form)

    form = {
        "user": "a",
        "value": "-1",
        "entities": "a.com\nb.com",
        "labels": "b2c\nhotdog",
        "entity_type": "company",
    }
    user, entities, labels, entity_type, value = parse_bulk_upload_v2_form(form)
    assert user == "a"
    assert value == -1
    assert len(entities) == 2
    assert len(labels) == 2
    assert entity_type == "company"

    form = {
        "user": "a",
        "value": "-2",
        "entities": "a.com\nb.com",
        "labels": "b2c\nhotdog",
        "entity_type": "company",
    }
    with pytest.raises(
        Exception, match=r"Value -2 is not in the list of acceptable annotations .*"
    ):
        parse_bulk_upload_v2_form(form)


def test__upsert_annotations(dbsession):
    entity_type = "company"
    entity = "123.com"
    user_id = 1
    label = "hotdog"
    value = 1
    context = {"text": "whatever"}
    annotation = ClassificationAnnotation(
        entity_type=entity_type,
        entity=entity,
        user_id=user_id,
        label=label,
        value=value,
        context=context,
    )
    dbsession.add(annotation)

    annotation_new = _upsert_annotation(
        dbsession=dbsession,
        entity_type=entity_type,
        entity=entity,
        label=label,
        user_id=user_id + 1,
        value=value,
    )
    dbsession.add(annotation_new)

    dbsession.commit()
    assert annotation_new.id != annotation.id
    assert annotation_new.user_id != annotation.user_id
    assert annotation_new.entity_type == annotation.entity_type
    assert annotation_new.entity == annotation.entity
    assert annotation_new.label == annotation.label
    assert annotation_new.value == annotation.value

    annotation_updated = _upsert_annotation(
        dbsession=dbsession,
        entity_type=entity_type,
        entity=entity,
        label=label,
        user_id=user_id,
        value=value * -1,
    )
    dbsession.add(annotation_new)
    dbsession.commit()
    assert annotation_updated.id == annotation.id
    assert annotation_updated.value == -1 * value
