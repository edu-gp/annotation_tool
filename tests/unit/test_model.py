from tests.sqlalchemy_conftest import *
from db.model import TextClassificationModel
import pytest
import sqlalchemy


def test_default_uuid_and_version(dbsession):
    model = TextClassificationModel()
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert len(model.uuid) > 0
    assert model.version == 1


def test_model_unique_on_uuid_and_version(dbsession):
    model = TextClassificationModel(uuid='123', version=1)
    dbsession.add(model)
    dbsession.commit()

    model = TextClassificationModel(uuid='123', version=1)
    dbsession.add(model)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        dbsession.commit()


def test_get_latest_version(dbsession):
    dbsession.add_all([
        TextClassificationModel(uuid='123', version=1),
        TextClassificationModel(uuid='123', version=2),
        TextClassificationModel(uuid='123', version=3),
    ])
    dbsession.commit()

    assert TextClassificationModel.get_latest_version(
        dbsession, uuid='123') == 3


def test_dir(dbsession):
    model = TextClassificationModel(uuid='123', version=1)
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert model.dir() == 'models/123/1'
