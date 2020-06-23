import pytest
import sqlalchemy
from mockito import when
from sqlalchemy.exc import ArgumentError
from tests.sqlalchemy_conftest import *
from tests.utils import fake_train_model
from db.model import TextClassificationModel, get_or_create, User, \
    get_active_model_for_label


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


def test_get_version(dbsession):
    dbsession.add_all([
        TextClassificationModel(uuid='123', version=1),
        TextClassificationModel(uuid='123', version=2),
        TextClassificationModel(uuid='123', version=3),
    ])
    dbsession.commit()

    assert TextClassificationModel.get_latest_version(
        dbsession, uuid='123') == 3

    assert TextClassificationModel.get_latest_version(
        dbsession, uuid='blah') is None

    assert TextClassificationModel.get_next_version(
        dbsession, uuid='123') == 4

    assert TextClassificationModel.get_next_version(
        dbsession, uuid='blah') == 1


def test_dir(dbsession):
    model = TextClassificationModel(uuid='123', version=1)
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert model.dir() == 'models/123/1'


def test_get_or_create(dbsession):
    user1 = get_or_create(dbsession=dbsession,
                          model=User,
                          username="user1")
    assert user1.username == "user1"

    when(dbsession).commit().thenRaise(ArgumentError())
    with pytest.raises(ArgumentError):
        _ = get_or_create(dbsession=dbsession,
                          model=User,
                          username="invalid_user_name")


def test_is_ready(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    model = TextClassificationModel(uuid='123', version=1)
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert model.is_ready() is False

    fake_train_model(model, str(tmp_path))

    assert model.is_ready() is True


def test_get_active_model_for_label(dbsession):
    label1 = "test_label1"

    model1 = TextClassificationModel(uuid='123', version=1, label=label1,
                                     is_active=True)
    model2 = TextClassificationModel(uuid='123', version=2, label=label1)

    dbsession.add_all([model1, model2])
    dbsession.commit()

    active_model1 = get_active_model_for_label(dbsession=dbsession,
                                              label=label1)

    assert active_model1.id == model1.id

    label2 = "test_label2"
    model3 = TextClassificationModel(uuid='1234', version=1, label=label2)
    model4 = TextClassificationModel(uuid='1234', version=2, label=label2)

    dbsession.add_all([model3, model4])
    dbsession.commit()

    active_model2 = get_active_model_for_label(dbsession=dbsession,
                                               label=label2)
    assert active_model2.id == model4.id

