from tests.sqlalchemy_conftest import *
from db.model import Task


def _populate_db(dbsession):
    task = Task(name='My Task', default_params={})
    dbsession.add(task)
    dbsession.commit()


def test_sanity(dbsession):
    _populate_db(dbsession)
    assert len(dbsession.query(Task).all()) == 1


def test_get_data_filenames(dbsession):
    task = Task.create(name='My Task')
    dbsession.add(task)
    dbsession.commit()
    dbsession.refresh(task)
    assert task.get_data_filenames() == []

    task = Task.create(name='My Task', data_filenames=['blah.csv'])
    dbsession.add(task)
    dbsession.commit()
    dbsession.refresh(task)
    assert task.get_data_filenames() == ['blah.csv']
