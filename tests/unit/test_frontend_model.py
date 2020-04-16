import frontend
from ar.data import fetch_labels_by_entity_type
from db.model import db, Label, EntityType
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
        labels = fetch_labels_by_entity_type(entity_type_name="company")
        for label in test_labels:
            assert label in labels
