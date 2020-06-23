from tests.sqlalchemy_conftest import *
from tests.utils import fake_train_model
from db.model import Task, TextClassificationModel


def _populate_db(dbsession):
    task = Task(name='My Task')
    dbsession.add(task)
    dbsession.commit()


def test_sanity(dbsession):
    _populate_db(dbsession)
    assert len(dbsession.query(Task).all()) == 1


def test_get_set(dbsession):
    # Note: Saving any modifications to JSON requires
    # marking them as modified with `flag_modified`.
    _populate_db(dbsession)

    task = dbsession.query(Task).first()
    assert task.get_patterns() == []
    task.set_patterns(['a', 'b', 'c'])
    print(task.default_params)
    dbsession.add(task)
    dbsession.commit()

    task = dbsession.query(Task).first()
    assert task.get_patterns() == ['a', 'b', 'c']


def test_create(dbsession):
    task = Task(name='My Task')
    task.set_patterns(['a', 'b', 'c'])
    dbsession.add(task)
    dbsession.commit()

    task = dbsession.query(Task).first()
    assert task.get_patterns() == ['a', 'b', 'c']
    assert task.get_uuid() is not None


def test_uuid_exists(dbsession):
    task = Task(name='My Task', default_params={})
    assert task.get_uuid() is not None
    dbsession.add(task)
    dbsession.commit()
    assert task.get_uuid() is not None

    task = dbsession.query(Task).first()
    assert task.get_uuid() is not None


def test_get_latest_and_active_model(dbsession, monkeypatch, tmp_path):
    monkeypatch.setenv('ALCHEMY_FILESTORE_DIR', str(tmp_path))

    task = Task(name='My Task', default_params={})

    assert task.get_latest_model() is None

    model = TextClassificationModel()
    task.text_classification_models.append(model)
    dbsession.add(task)
    dbsession.commit()

    assert task.get_latest_model() is not None

    fake_train_model(model, str(tmp_path))

    assert task.get_latest_model() is not None
