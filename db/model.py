from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.schema import ForeignKey, Column
from sqlalchemy.types import Integer, String, JSON, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# This is a flask_sqlalchemy object; Only for use inside Flask
db = SQLAlchemy(model_class=Base)


class Label(Base):
    __tablename__ = 'label'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)
    # A label can be part of many annotations.
    annotations = relationship('Annotation', backref='label',
                               lazy='dynamic')
    entity_type_id = Column(Integer, ForeignKey('entity_type.id'))


class EntityType(Base):
    __tablename__ = 'entity_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)
    # TODO how should we setup the lazy loading mode?
    labels = relationship('Label', backref='entity_type', lazy='dynamic')
    entities = relationship('Entity', backref='entity_type', lazy='dynamic')

    def __repr__(self):
        return '<EntityType {}>'.format(self.name)


class Entity(Base):
    __tablename__ = 'entity'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)
    # An entity can have many annotations on it.
    annotations = relationship('Annotation', backref='entity',
                               lazy='dynamic')
    entity_type_id = Column(Integer, ForeignKey('entity_type.id'))

    def __repr__(self):
        return '<Entity {}>'.format(self.name)


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(64), index=True, unique=True,
                      nullable=False)
    # A user can do many annotations.
    annotations = relationship('Annotation', backref='user', lazy='dynamic')

    def __repr__(self):
        return '<User {}>'.format(self.username)


class Context(Base):
    __tablename__ = 'context'

    id = Column(Integer, primary_key=True)
    hash = Column(String(128), index=True, unique=True, nullable=False)
    data = Column(JSON, nullable=False)
    # A context can be part of many annotations.
    annotations = relationship('Annotation', backref='context',
                               lazy='dynamic')

    def __repr__(self):
        return '<Context {}: {}>'.format(self.id, self.data)


class Annotation(Base):
    __tablename__ = 'annotation'

    id = Column(Integer, primary_key=True)
    value = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    last_updated_at = Column(DateTime, nullable=False)

    entity_id = Column(Integer, ForeignKey('entity.id'))
    label_id = Column(Integer, ForeignKey('label.id'))
    user_id = Column(Integer, ForeignKey('user.id'))
    context_id = Column(Integer, ForeignKey('context.id'))

    def __repr__(self):
        return """
        Annotation {}:
        Entity Id {}, 
        Label Id {},
        User Id {},
        Context Id {},
        Value {},
        Created at {},
        Last Updated at {}
        """.format(
            self.id,
            self.entity_id,
            self.label_id,
            self.user_id,
            self.context_id,
            self.value,
            self.created_at,
            self.last_updated_at
        )
