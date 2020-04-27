from tests.sqlalchemy_conftest import *
from db.model import (
    User, EntityType, Entity, Label, Context, ClassificationAnnotation,
    ClassificationTrainingData
)


def test_get_or_create(dbsession):
    Context.get_or_create(dbsession, {"text": "a"})
    Context.get_or_create(dbsession, {"text": "a"})
    Context.get_or_create(dbsession, {"text": "b"})

    assert dbsession.query(Context).count() == 2


def test_data_is_json_type(dbsession):
    Context.get_or_create(dbsession, {"text": "a"})

    ctx = dbsession.query(Context).first()
    assert isinstance(ctx.data, dict), type(ctx.data)
