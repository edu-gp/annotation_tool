from tests.sqlalchemy_conftest import *
from db.model import Task


def _populate_db(dbsession):
    task = Task(name='My Task', default_params={})
    dbsession.add(task)
    dbsession.commit()


def test_sanity(dbsession):
    _populate_db(dbsession)
    assert len(dbsession.query(Task).all()) == 1
