import sqlalchemy
from mockito import when
from sqlalchemy.exc import ArgumentError

from alchemy.db.model import TextClassificationModel, User, get_or_create
from tests.sqlalchemy_conftest import *
from tests.utils import fake_train_model


def test_default_uuid_and_version(dbsession):
    model = TextClassificationModel()
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert len(model.uuid) > 0
    assert model.version == 1


def test_model_unique_on_uuid_and_version(dbsession):
    model = TextClassificationModel(uuid="123", version=1)
    dbsession.add(model)
    dbsession.commit()

    model = TextClassificationModel(uuid="123", version=1)
    dbsession.add(model)
    with pytest.raises(sqlalchemy.exc.IntegrityError):
        dbsession.commit()


def test_get_version(dbsession):
    dbsession.add_all(
        [
            TextClassificationModel(uuid="123", version=1),
            TextClassificationModel(uuid="123", version=2),
            TextClassificationModel(uuid="123", version=3),
        ]
    )
    dbsession.commit()

    assert TextClassificationModel.get_latest_version(dbsession, uuid="123") == 3

    assert TextClassificationModel.get_latest_version(dbsession, uuid="blah") is None

    assert TextClassificationModel.get_next_version(dbsession, uuid="123") == 4

    assert TextClassificationModel.get_next_version(dbsession, uuid="blah") == 1


def test_dir(dbsession):
    model = TextClassificationModel(uuid="123", version=1)
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert model.dir.endswith("models/123/1")


def test_get_or_create(dbsession):
    user1 = get_or_create(dbsession=dbsession, model=User, username="user1")
    assert user1.username == "user1"

    when(dbsession).commit().thenRaise(ArgumentError())
    with pytest.raises(ArgumentError):
        _ = get_or_create(dbsession=dbsession, model=User, username="invalid_user_name")


def test_is_ready(dbsession, monkeypatch, tmp_path):
    data_store = 'cloud'
    monkeypatch.setenv("STORAGE_BACKEND", data_store)
    if data_store == 'cloud':
        tmp_path = '__filestore'
    monkeypatch.setenv("ALCHEMY_FILESTORE_DIR", str(tmp_path))

    model = TextClassificationModel(uuid="123", version=1)
    dbsession.add(model)
    dbsession.commit()
    dbsession.refresh(model)

    assert model.is_ready(data_store=data_store) is False

    fake_train_model(model, str(tmp_path), data_store=data_store)

    assert model.is_ready(data_store=data_store) is True
