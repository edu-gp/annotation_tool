import logging
import copy
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import ForeignKey, Column
from sqlalchemy.types import Integer, Float, String, JSON, DateTime, Text
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

# =============================================================================
# DB Access

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

        self.session = session
        self.engine = engine


# =============================================================================
# Enums

class AnnotationRequestStatus:
    Pending = 0
    Complete = 1
    Stale = 2


class AnnotationType:
    ClassificationAnnotation = 1


class JobStatus:
    Init = "init"
    Complete = "complete"
    Failed = "failed"


class EntityTypeEnum:
    COMPANY = "company"


# =============================================================================
# Tables


class Label(Base):
    __tablename__ = 'label'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)
    # A label can be part of many annotations.
    classification_annotations = relationship('ClassificationAnnotation',
                                              back_populates='label',
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
                                              back_populates='user',
                                              lazy='dynamic')

    def __repr__(self):
        return '<User {}>'.format(self.username)

    def fetch_ar_count_per_task(self):
        """Returns a list of tuples (name, task_id, count)
        """
        session = inspect(self).session

        q = session.query(
            Task.name, Task.id, func.count(Task.id)
        ).join(AnnotationRequest) \
            .filter(AnnotationRequest.task_id == Task.id) \
            .filter(AnnotationRequest.user == self) \
            .group_by(Task.id) \
            .order_by(Task.id)

        return q.all()

    def fetch_ar_for_task(self, task_id,
                          status=AnnotationRequestStatus.Pending):
        """Returns a list of AnnotationRequest objects"""
        session = inspect(self).session

        q = session.query(AnnotationRequest) \
            .filter(AnnotationRequest.task_id == task_id) \
            .filter(AnnotationRequest.user == self) \
            .filter(AnnotationRequest.status == status) \
            .order_by(AnnotationRequest.order)

        return q.all()


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
    label = relationship("Label", back_populates="classification_annotations")

    user_id = Column(Integer, ForeignKey('user.id'))
    user = relationship("User", back_populates="classification_annotations")

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


class ClassificationTrainingData(Base):
    __tablename__ = 'classification_training_data'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    label_id = Column(Integer, ForeignKey('label.id'), nullable=False)
    label = relationship("Label")

    output_filename = Column(Text, nullable=False)


class Model(Base):
    __tablename__ = 'model'

    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    params = Column(JSON, nullable=False, default=lambda: {})
    output = Column(JSON, nullable=False, default=lambda: {})

    # All the inferences we have ran on files
    file_inferences = relationship("FileInference", back_populates="model")

    # Optionally associated with a Task
    task_id = Column(Integer, ForeignKey('task.id'))
    task = relationship("Task", back_populates="models")

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'model'
    }

    def __repr__(self):
        return f'<Model:{self.type}>'


class TextClassificationModel(Model):
    __mapper_args__ = {
        'polymorphic_identity': 'text_classification_model'
    }


class FileInference(Base):
    __tablename__ = 'file_inference'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    model_id = Column(Integer, ForeignKey('model.id'), nullable=False)
    model = relationship("Model", back_populates="file_inferences")

    # We want to be able to see, for a given file, all the inferences on it.
    input_filename = Column(Text, index=True, nullable=False)
    output_filename = Column(Text, nullable=False)


class Task(Base):
    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    default_params = Column(JSON, nullable=False)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    models = relationship("Model", back_populates="task")
    text_classification_models = relationship("TextClassificationModel")


class AnnotationRequest(Base):
    __tablename__ = 'annotation_request'

    # --------- REQUIRED ---------

    id = Column(Integer, primary_key=True)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Who should annotate.
    user_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    user = relationship("User")

    # What should the user annotate.
    context_id = Column(Integer, ForeignKey('context.id'), nullable=False)
    context = relationship("Context")

    # What kind of annotation should the user be performing.
    # See the AnnotationType enum.
    annotation_type = Column(Integer, nullable=False)

    # AnnotationRequestStatus
    status = Column(Integer, index=True, nullable=False,
                    default=AnnotationRequestStatus.Pending)

    # --------- OPTIONAL ---------

    # Which task this request belongs to, so we can list all requests per task.
    # (If null, this request does not belong to any task)
    task_id = Column(Integer, ForeignKey('task.id'))
    task = relationship("Task")

    # How the user should prioritize among many requests.
    # Index these because we will order by them.
    order = Column(Float, index=True)

    # --------- INFORMATIONAL ---------

    # Friendly name to show to the user
    name = Column(String)

    # Additional info to show to the user, e.g. the score, probability, etc.
    additional_info = Column(JSON)

    # Where this request came from. e.g. {'source': BackgroundJob, 'id': 123}
    source = Column(JSON)


# =============================================================================
# Convenience Functions


def get_or_create(dbsession, model, exclude_keys_in_retrieve=None, **kwargs):
    """Retrieve an instance from the database based on key and value
    specified in kwargs but excluding those in the exclude_keys_in_retrieve.

    :param dbsession: database session
    :param model: The db model class name
    :param exclude_keys_in_retrieve: keys to exclude in retrieve
    :param kwargs: key-value pairs to retrieve or create an instance
    :return: a model instance
    """
    if exclude_keys_in_retrieve is None:
        exclude_keys_in_retrieve = []
    read_kwargs = copy.copy(kwargs)
    for key in exclude_keys_in_retrieve:
        read_kwargs.pop(key, None)

    instance = dbsession.query(model).filter_by(**read_kwargs).one_or_none()
    if instance:
        return instance
    else:
        instance = model(**kwargs)
        dbsession.add(instance)
        dbsession.commit()
        logging.info("Created a new instance of {}".format(instance))
        return instance


def fetch_labels_by_entity_type(dbsession, entity_type_name):
    """Fetch all labels for an entity type.

    :param entity_type_name: the entity type name
    :return: all the labels under the entity type
    """
    labels = dbsession.query(Label).join(EntityType) \
        .filter(EntityType.name == entity_type_name).all()
    return [label.name for label in labels]


def save_labels_by_entity_type(dbsession, entity_type_name, label_names):
    """Update labels under the entity type.

    If entity type doesn't exist, create it first.

    :param entity_type_name: the entity type name
    :param label_names: labels to be saved
    """
    logging.info("Finding the EntityType for {}".format(entity_type_name))
    entity_type = get_or_create(dbsession, EntityType, name=entity_type_name)
    labels = [Label(name=name, entity_type_id=entity_type.id)
              for name in label_names]
    dbsession.add_all(labels)
    dbsession.commit()
