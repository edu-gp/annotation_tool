import frontend
from db.model import db, Label, EntityType
from db.config import TestingConfig
from flask_testing import TestCase

test_labels = ["B2C", "HEALTHCARE"]
entity_type_name = "company"


class ModelDeletionBehaviorTest(TestCase):

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
        entity_type = EntityType(name=entity_type_name)
        db.session.add(entity_type)
        db.session.commit()
        labels = [Label(name=label, entity_type_id=entity_type.id) for
                  label in test_labels]
        db.session.bulk_save_objects(labels)
        db.session.commit()

    def test_delete_entity_type(self):
        entity_type = EntityType.query.filter_by(name=entity_type_name).first()
        labels = entity_type.labels.all()
        label_ids = [label.id for label in labels]
        for label in labels:
            assert label.entity_type_id == entity_type.id

        db.session.delete(entity_type)
        db.session.commit()

        entity_type2 = EntityType.query.filter_by(
            name=entity_type_name).first()
        assert entity_type2 is None
        for id in label_ids:
            label = Label.query.filter_by(id=id).first()
            assert label.entity_type_id is None
