from tests.sqlalchemy_conftest import *
from db.model import Task


def _populate_db(dbsession):
    task = Task(name='My Task', default_params={})
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
    task = Task(name='My Task', default_params={})
    task.set_patterns(['a', 'b', 'c'])
    dbsession.add(task)
    dbsession.commit()

    task = dbsession.query(Task).first()
    assert task.get_patterns() == ['a', 'b', 'c']
