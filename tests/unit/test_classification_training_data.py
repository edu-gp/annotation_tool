from tests.sqlalchemy_conftest import *
from db.model import (
    User, EntityType, Entity, Label, Context, ClassificationAnnotation,
    ClassificationTrainingData
)


def test_path(dbsession):
    label = Label(name='hello')
    data = ClassificationTrainingData(label=label)
    dbsession.add_all([data, label])
    dbsession.commit()
    dbsession.refresh(data)

    assert data.path() is not None
    assert data.path().endswith('.jsonl')
