from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy.schema import ForeignKey, Column
from sqlalchemy.types import Integer, String, JSON, DateTime
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

# This is a flask_sqlalchemy object; Only for use inside Flask
db = SQLAlchemy(model_class=Base)
metadata = db.Model.metadata


class Database:
    """For accessing database outside of Flask."""

    def __init__(self, db_uri):
        engine = create_engine(db_uri)
        session_factory = sessionmaker(bind=engine)

        # scoped_session makes sure the factory returns a singleton session
        # instance per scope, until you explicitly call session.remove().
        session = scoped_session(session_factory)

        # Adds Query Property to Models - enables `User.query.query_method()`
        Base.query = session.query_property()

        self.session = session
        self.engine = engine

    def create_all(self):
        Base.metadata.create_all(bind=self.engine)

    def drop_all(self):
        Base.metadata.drop_all(bind=self.engine)


class Label(Base):
    __tablename__ = 'label'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)
    # A label can be part of many annotations.
    classification_annotations = relationship('ClassificationAnnotation',
                                              backref='label', lazy='dynamic')
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
    classification_annotations = relationship('ClassificationAnnotation',
                                              backref='entity', lazy='dynamic')
    entity_type_id = Column(Integer, ForeignKey('entity_type.id'))

    def __repr__(self):
        return '<Entity {}>'.format(self.name)


class User(Base):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String(64), index=True, unique=True,
                      nullable=False)
    # A user can do many annotations.
    classification_annotations = relationship('ClassificationAnnotation',
                                              backref='user', lazy='dynamic')

    def __repr__(self):
        return '<User {}>'.format(self.username)


class Context(Base):
    __tablename__ = 'context'

    id = Column(Integer, primary_key=True)
    hash = Column(String(128), index=True, unique=True, nullable=False)
    data = Column(JSON, nullable=False)
    # A context can be part of many annotations.
    classification_annotations = relationship('ClassificationAnnotation',
                                              backref='context',
                                              lazy='dynamic')

    def __repr__(self):
        return '<Context {}: {}>'.format(self.id, self.data)


class ClassificationAnnotation(Base):
    __tablename__ = 'classification_annotation'

    id = Column(Integer, primary_key=True)
    value = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    entity_id = Column(Integer, ForeignKey('entity.id'))
    label_id = Column(Integer, ForeignKey('label.id'))
    user_id = Column(Integer, ForeignKey('user.id'))
    context_id = Column(Integer, ForeignKey('context.id'))

    def __repr__(self):
        return """
        Classification Annotation {}:
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
            self.updated_at
        )


class JobType:
    AnnotationRequestGenerator = 1
    TextClassificationModelTraining = 2


class JobStatus:
    INIT = "init"


class EntityTypeEnum:
    COMPANY = "company"


class BackgroundJob(Base):
    __tablename__ = 'background_job'

    id = Column(Integer, primary_key=True)
    type = Column(Integer, index=True, nullable=False)
    params = Column(JSON, nullable=False)
    output = Column(JSON, nullable=False)
    status = Column(String(64), nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # A BackgroundJob can optionally be associated with a Task
    task_id = Column(Integer, ForeignKey('task.id'))
    task = relationship("Task", back_populates="background_jobs")

    def __repr__(self):
        return f'<BackgroundJob:{self.type}>'


class Task(Base):
    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    default_params = Column(JSON, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Useful for adding a new job, eg. task.background_jobs.append(job)
    background_jobs = relationship("BackgroundJob", back_populates="task")

    # Useful for fetching different types of jobs
    annotation_request_generator_jobs = relationship(
        "BackgroundJob",
        primaryjoin="and_(Task.id==BackgroundJob.task_id, "
        f"BackgroundJob.type=={JobType.AnnotationRequestGenerator})")

    text_classification_model_training_jobs = relationship(
        "BackgroundJob",
        primaryjoin="and_(Task.id==BackgroundJob.task_id, "
        f"BackgroundJob.type=={JobType.TextClassificationModelTraining})")

