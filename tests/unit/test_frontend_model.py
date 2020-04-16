import pytest

import frontend
from ar.data import fetch_labels_by_entity_type
from db.model import db, Label, EntityType
from frontend.config import TestingConfig
from flask_testing import TestCase


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
        label1 = Label(name="B2C", entity_type_id=entity_type.id)
        label2 = Label(name="HEALTHCARE", entity_type_id=entity_type.id)
        db.session.bulk_save_objects([label1, label2])
        db.session.commit()

    def test_fetch_labels_by_entity_type(self):
        labels = fetch_labels_by_entity_type(entity_type_name="company")
        assert "B2C" in labels
        assert "HEALTHCARE" in labels

