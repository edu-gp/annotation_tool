from tests.sqlalchemy_conftest import *
from db.model import (
    Label, EntityType,
    fetch_labels_by_entity_type, save_labels_by_entity_type
)

test_labels = ["B2C", "HEALTHCARE"]


def _populate_db(dbsession):
    entity_type = EntityType(name="company")
    dbsession.add(entity_type)
    dbsession.commit()
    labels = [Label(name=label, entity_type_id=entity_type.id) for
              label in test_labels]
    dbsession.bulk_save_objects(labels)
    dbsession.commit()


def test_fetch_labels_by_entity_type(dbsession):
    _populate_db(dbsession)

    labels = fetch_labels_by_entity_type(dbsession, "company")
    for label in test_labels:
        assert label in labels


def test_fetch_labels_for_unknown_entity_type(dbsession):
    _populate_db(dbsession)

    labels = fetch_labels_by_entity_type(dbsession, "unknown")
    assert len(labels) == 0


def test_save_labels_for_existing_entity_type(dbsession):
    _populate_db(dbsession)

    new_labels = ["logstic"]
    entity_type_name = "company"
    save_labels_by_entity_type(dbsession, entity_type_name, new_labels)

    exisiting_labels = fetch_labels_by_entity_type(dbsession, "company")

    expected_label_set = set(test_labels)
    expected_label_set.update(new_labels)

    assert set(exisiting_labels) == expected_label_set


def test_save_labels_for_new_entity_type(dbsession):
    _populate_db(dbsession)

    new_labels = ["startup", "x-googler"]
    entity_type_name = "people"
    save_labels_by_entity_type(dbsession, entity_type_name, new_labels)

    exisiting_labels = fetch_labels_by_entity_type(dbsession, entity_type_name)

    expected_label_set = set(new_labels)

    assert set(exisiting_labels) == expected_label_set
