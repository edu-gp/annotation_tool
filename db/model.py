import logging
import copy
import os
from werkzeug.utils import secure_filename
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, inspect, UniqueConstraint, MetaData
from sqlalchemy.schema import ForeignKey, Column
from sqlalchemy.types import Integer, Float, String, JSON, DateTime, Text
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from shared.utils import (
    gen_uuid, stem, file_len, load_json, load_jsonl, safe_getattr
)
from db.fs import (
    filestore_base_dir, RAW_DATA_DIR, MODELS_DIR, TRAINING_DATA_DIR
)
from train.no_deps.paths import (
    _get_config_fname, _get_data_parser_fname, _get_metrics_fname,
    _get_all_plots, _get_exported_data_fname, _get_all_inference_fnames
)

meta = MetaData(naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s"
      })
Base = declarative_base(metadata=meta)

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

    def file_friendly_name(self):
        return secure_filename(self.name)


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

    def path(self):
        return os.path.join(TRAINING_DATA_DIR, self.label.file_friendly_name(),
                            str(int(self.created_at.timestamp())) + '.jsonl')

    def length(self):
        return file_len(self.path())


class Model(Base):
    __tablename__ = 'model'

    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # It's useful to submit jobs from various systems to the same remote
    # training system. UUID makes sure those jobs don't clash.
    uuid = Column(String(64), index=True, nullable=False, default=gen_uuid)
    version = Column(Integer, index=True, nullable=False, default=1)

    data = Column(JSON, nullable=False, default=lambda: {})

    # All the inferences we have ran on files
    file_inferences = relationship("FileInference", back_populates="model")

    # Optionally associated with a Task
    task_id = Column(Integer, ForeignKey('task.id'))
    task = relationship("Task", back_populates="models")

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'model'
    }

    __table_args__ = (
        UniqueConstraint('uuid', 'version', name='_uuid_version_uc'),
    )

    def __repr__(self):
        return f'<Model:{self.type}:{self.uuid}:{self.version}>'

    @staticmethod
    def get_latest_version(dbsession, uuid):
        return dbsession.query(Model.version) \
            .filter(Model.uuid == uuid) \
            .order_by(Model.version.desc()).first()[0]

    def dir(self):
        """Returns the directory location relative to the filestore root"""
        return os.path.join(MODELS_DIR, self.uuid, str(self.version))

    def inference_dir(self):
        return os.path.join(self.dir(), "inference")

    def _load_json(self, fname_fn):
        model_dir = os.path.join(filestore_base_dir(), self.dir())
        fname = fname_fn(model_dir)
        if os.path.isfile(fname):
            return load_json(fname)
        else:
            return None

    def get_metrics(self):
        return self._load_json(_get_metrics_fname)

    def get_config(self):
        return self._load_json(_get_config_fname)

    def get_data_parser(self):
        return self._load_json(_get_data_parser_fname)

    def get_plots(self):
        """Return a list of urls for plots"""
        model_dir = os.path.join(filestore_base_dir(), self.dir())
        return _get_all_plots(model_dir)

    def get_inference_fname_paths(self):
        model_dir = os.path.join(filestore_base_dir(), self.dir())
        return _get_all_inference_fnames(model_dir)

    def get_inference_fnames(self):
        """Special function to put together information for the UI"""
        return [stem(path) + '.jsonl'
                for path in self.get_inference_fname_paths()]

    def get_len_data(self):
        """Return how many datapoints were used to train this model.
        We measure the size of the file in the model directory, not to be
        confused with the file from a ClassificationTrainingData instance!
        """
        model_dir = os.path.join(filestore_base_dir(), self.dir())
        fname = _get_exported_data_fname(model_dir)
        return file_len(fname)


class TextClassificationModel(Model):
    __mapper_args__ = {
        'polymorphic_identity': 'text_classification_model'
    }

    def __str__(self):
        return f'TextClassificationModel:{self.uuid}:v{self.version}'


class FileInference(Base):
    __tablename__ = 'file_inference'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    model_id = Column(Integer, ForeignKey('model.id'), nullable=False)
    model = relationship("Model", back_populates="file_inferences")

    # We want to be able to query, for a given file, all the inferences on it.
    input_filename = Column(Text, index=True, nullable=False)

    def path(self):
        return os.path.join(self.model.inference_dir(),
                            stem(self.input_filename) + '.pred.npy')

    def create_exported_dataframe(self):
        from train.no_deps.inference_results import InferenceResults

        _base = filestore_base_dir()

        # Load Inference Results
        ir = InferenceResults.load(os.path.join(_base, self.path()))

        # Load Original Data
        df = load_jsonl(
            os.path.join(_base, RAW_DATA_DIR, self.input_filename), to_df=True)

        # Check they're the same size
        assert len(df) == len(ir.probs)

        # Combine the two together.
        df['probs'] = ir.probs
        df['domain'] = df['meta'].apply(lambda x: x.get('domain'))
        df['name'] = df['meta'].apply(lambda x: x.get('name'))
        # Note: We don't keep the 'text' column on purpose!
        df = df[['name', 'domain', 'probs']]

        return df


class Task(Base):
    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)
    default_params = Column(JSON, nullable=False)
    """
    Example default_params:
    {
        "data_filenames": [
            "my_data.jsonl"
        ],
        "annotators": [
            "ann", "ben"
        ],
        "labels": [
            "hotdog"
        ],
        "patterns_file": "my_patterns.jsonl",
        "patterns": ["bun", "sausage"]
    }
    """

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    models = relationship("Model", back_populates="task")
    text_classification_models = relationship(
        "TextClassificationModel",
        order_by="desc(TextClassificationModel.version)",
        lazy="dynamic")

    def __str__(self):
        return self.name

    def get_labels(self):
        return self.default_params.get('labels', [])

    def get_annotators(self):
        return self.default_params.get('annotators', [])

    def get_data_filenames(self):
        return self.default_params.get('data_filenames', [])

    def get_pattern_model(self):
        from inference.pattern_model import PatternModel
        from db.task import _convert_to_spacy_patterns

        if safe_getattr(self, '__cached_pattern_model') is None:
            patterns = []

            _patterns_file = self.default_params.get('patterns_file')
            if _patterns_file:
                patterns += load_jsonl(
                    os.path.join(filestore_base_dir(),
                                 RAW_DATA_DIR, _patterns_file),
                    to_df=False)

            _patterns = self.default_params.get('patterns')
            if _patterns is not None:
                patterns += _convert_to_spacy_patterns(_patterns)

            self.__cached_pattern_model = PatternModel(patterns)

        return self.__cached_pattern_model

    def get_active_nlp_model(self):
        # TODO refactor this to a different name later (e.g. get_latest_model)
        from inference.nlp_model import NLPModel

        latest_model = self.text_classification_models.first()

        if latest_model is not None:
            return NLPModel(latest_model.uuid, latest_model.version)
        else:
            return None


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

    classification_annotation_id = Column(Integer, ForeignKey(
        'classification_annotation.id'))
    classification_annotation = relationship("ClassificationAnnotation",
                                             uselist=False)

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
