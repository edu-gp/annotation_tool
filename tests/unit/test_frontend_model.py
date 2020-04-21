import frontend
from db.model import (
    db, Label, EntityType,
    fetch_labels_by_entity_type, save_labels_by_entity_type
)
from db.config import TestingConfig
from flask_testing import TestCase

test_labels = ["B2C", "HEALTHCARE"]


class FrontEndModelTest(TestCase):

    def create_app(self):
        app = frontend.create_app(TestingConfig)
        return app

    def setUp(self) -> None:
        db.create_all()
        self.populate_db()

    def tearDown(self) -> None:
        db.session.remove()
        db.drop_all()

    def populate_db(self):
        entity_type = EntityType(name="company")
        db.session.add(entity_type)
        db.session.commit()
        labels = [Label(name=label, entity_type_id=entity_type.id) for
                  label in test_labels]
        db.session.bulk_save_objects(labels)
        db.session.commit()

    def test_fetch_labels_by_entity_type(self):
        labels = fetch_labels_by_entity_type(db.session, "company")
        for label in test_labels:
            assert label in labels

    def test_fetch_labels_for_unknown_entity_type(self):
        labels = fetch_labels_by_entity_type(db.session, "unknown")
        assert len(labels) == 0

    def test_save_labels_for_existing_entity_type(self):
        new_labels = ["logstic"]
        entity_type_name = "company"
        save_labels_by_entity_type(db.session, entity_type_name, new_labels)

        exisiting_labels = fetch_labels_by_entity_type(
            db.session, "company")

        expected_label_set = set(test_labels)
        expected_label_set.update(new_labels)

        assert set(exisiting_labels) == expected_label_set

    def test_save_labels_for_new_entity_type(self):
        new_labels = ["startup", "x-googler"]
        entity_type_name = "people"
        save_labels_by_entity_type(db.session, entity_type_name, new_labels)

        exisiting_labels = fetch_labels_by_entity_type(
            db.session, entity_type_name)

        expected_label_set = set(new_labels)

        assert set(exisiting_labels) == expected_label_set
