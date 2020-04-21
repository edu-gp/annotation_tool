# Based on: https://gist.github.com/kissgyorgy/e2365f25a213de44b9a2

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from db.model import Base as BaseModel
from db.config import TestingConfig
import pytest


@pytest.fixture(scope='session')
def engine():
    return create_engine(TestingConfig.SQLALCHEMY_DATABASE_URI)


@pytest.yield_fixture(scope='session')
def tables(engine):
    BaseModel.metadata.create_all(engine)
    yield
    BaseModel.metadata.drop_all(engine)


@pytest.yield_fixture
def dbsession(engine, tables):
    """Returns an sqlalchemy session, and after the test tears down everything properly."""
    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction
    session = Session(bind=connection)

    yield session

    session.close()
    # roll back the broader transaction
    transaction.rollback()
    # put back the connection to the connection pool
    connection.close()
