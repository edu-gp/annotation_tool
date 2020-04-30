import logging
import copy
import os
from typing import List
from werkzeug.utils import secure_filename
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, inspect, UniqueConstraint, MetaData
from sqlalchemy.schema import ForeignKey, Column
from sqlalchemy.types import Integer, Float, String, JSON, DateTime, Text
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from shared.utils import (
    gen_uuid, stem, file_len, load_json, load_jsonl, safe_getattr)
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

    @staticmethod
    def from_config(config):
        return Database(config.SQLALCHEMY_DATABASE_URI)

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


class AnnotationValue:
    POSITIVE = 1
    NEGTIVE = -1
    UNSURE = 0
    NOT_ANNOTATED = -2


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
    entity_type = relationship("EntityType", back_populates="labels")

    def file_friendly_name(self):
        return secure_filename(self.name)


class EntityType(Base):
    __tablename__ = 'entity_type'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)
    labels = relationship(
        'Label', back_populates='entity_type', lazy='dynamic')
    entities = relationship(
        'Entity', back_populates='entity_type', lazy='dynamic')

    def __repr__(self):
        return '<EntityType {}>'.format(self.name)


class Entity(Base):
    __tablename__ = 'entity'

    id = Column(Integer, primary_key=True)
    name = Column(String(64), index=True, unique=True, nullable=False)

    entity_type_id = Column(Integer, ForeignKey('entity_type.id'))
    entity_type = relationship("EntityType", back_populates="entities")

    # An entity can have many annotations on it.
    classification_annotations = relationship(
        'ClassificationAnnotation', back_populates='entity', lazy='dynamic')

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


class ClassificationAnnotation(Base):
    __tablename__ = 'classification_annotation'

    id = Column(Integer, primary_key=True)
    value = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    entity_id = Column(Integer, ForeignKey('entity.id'))
    entity = relationship(
        "Entity", back_populates="classification_annotations")

    label_id = Column(Integer, ForeignKey('label.id'))
    label = relationship("Label", back_populates="classification_annotations")

    user_id = Column(Integer, ForeignKey('user.id'))
    user = relationship("User", back_populates="classification_annotations")

    context = Column(JSON)
    """
    e.g. Currently the context looks like:
    {
        "text": "A quick brown fox.",
        "meta": {
            "name": "Blah",
            "domain": "foo.com"
        }
    }
    """

    def __repr__(self):
        return """
        Classification Annotation {}:
        Entity Id {},
        Label Id {},
        User Id {},
        Value {},
        Created at {},
        Last Updated at {}
        """.format(
            self.id,
            self.entity_id,
            self.label_id,
            self.user_id,
            self.value,
            self.created_at,
            self.updated_at
        )


def majority_vote_annotations_query(dbsession, label):
    """
    Returns a query that fetches a list of 3-tuples
    [(entity_name, anno_value, count), ...]
    where the annotation value is the most common for that entity name
    associated with the given label.

    For example, if we have 3 annotations for the entity X with values
    [1, -1, -1], then one of the elements this query would return would be
    (X, -1, 2)

    Note: This query ignores annotation values of 0 - they are "Unknown"s.
    """
    subquery = dbsession.query(
        Entity.name,
        ClassificationAnnotation.value,
        func.count('*').label('count')
    ) \
        .join(Label).filter_by(id=label.id) \
        .join(Entity).filter(Entity.id == ClassificationAnnotation.entity_id) \
        .filter(ClassificationAnnotation.value != 0) \
        .group_by(Entity.id, ClassificationAnnotation.value) \
        .subquery()

    query = dbsession.query(
        subquery.c.name,
        subquery.c.value,
        func.max(subquery.c.count)
    ).group_by(subquery.c.name)

    return query


class ClassificationTrainingData(Base):
    # TODO rename to BinaryTextClassificationTrainingData
    """
    This points to a jsonl file where each line is of the structure:
    {"text": "A quick brown fox", "labels": {"bear": -1}}
    """
    __tablename__ = 'classification_training_data'

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    label_id = Column(Integer, ForeignKey('label.id'), nullable=False)
    label = relationship("Label")

    @staticmethod
    def create_for_label(dbsession, label: Label,
                         entity_text_lookup_fn, batch_size=50):
        """
        Create a training data for the given label by taking a snapshot of all
        the annotations created with it so far.
        Inputs:
            dbsession: -
            label: -
            entity_text_lookup_fn: A function that, when given the
                entity_type_id and entity_name, returns a piece of text that
                about the entity that we can use for training.
            batch_size: Database query batch size.
        """
        query = majority_vote_annotations_query(dbsession, label)

        final = []
        for ent_name, anno_value, count in query.yield_per(batch_size):
            final.append({
                'text': entity_text_lookup_fn(label.entity_type_id, ent_name),
                'labels': {label.name: anno_value}
            })

        # Save the database object, use it to generate filename, then save the
        # file on disk.
        data = ClassificationTrainingData(label=label)
        dbsession.add(data)
        dbsession.commit()

        output_fname = os.path.join(filestore_base_dir(), data.path())
        os.makedirs(os.path.dirname(output_fname), exist_ok=True)
        from shared.utils import save_jsonl
        save_jsonl(output_fname, final)

        return data

    def path(self, abs=False):
        p = os.path.join(TRAINING_DATA_DIR, self.label.file_friendly_name(),
                         str(int(self.created_at.timestamp())) + '.jsonl')
        if abs:
            p = os.path.join(filestore_base_dir(), p)
        return p

    def load_data(self, to_df=False):
        path = self.path(abs=True)
        return load_jsonl(path, to_df=to_df)

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

    classification_training_data_id = Column(Integer, ForeignKey(
        'classification_training_data.id'))
    classification_training_data = relationship("ClassificationTrainingData")

    config = Column(JSON)

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
        res = dbsession.query(Model.version) \
            .filter(Model.uuid == uuid) \
            .order_by(Model.version.desc()).first()
        if res is None:
            version = None
        else:
            version = res[0]
        return version

    @staticmethod
    def get_next_version(dbsession, uuid):
        version = Model.get_latest_version(dbsession, uuid)
        if version is None:
            return 1
        else:
            return version + 1

    def dir(self, abs=False):
        """Returns the directory location relative to the filestore root"""
        dir = os.path.join(MODELS_DIR, self.uuid, str(self.version))
        if abs:
            dir = os.path.join(filestore_base_dir(), dir)
        return dir

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

    def create_exported_dataframe(self, include_text=False):
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
        # TODO we must use these fields. Can we make `meta` more flexible?
        df['domain'] = df['meta'].apply(lambda x: x.get('domain'))
        df['name'] = df['meta'].apply(lambda x: x.get('name'))
        if include_text:
            df = df[['name', 'domain', 'text', 'probs']]
        else:
            df = df[['name', 'domain', 'probs']]

        return df


class Task(Base):
    __tablename__ = 'task'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=False)

    # Note: Saving any modifications to JSON requires
    # marking them as modified with `flag_modified`.
    default_params = Column(JSON, nullable=False)
    """
    Example default_params:
    {
        "uuid": ...,
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

    def __init__(self, *args, **kwargs):
        # Set default
        default_params = kwargs.get('default_params', {})
        if 'uuid' not in default_params:
            default_params['uuid'] = gen_uuid()
        kwargs['default_params'] = default_params
        super(Task, self).__init__(*args, **kwargs)

    def __str__(self):
        return self.name

    def set_labels(self, labels: List[str]):
        self.default_params['labels'] = labels
        flag_modified(self, 'default_params')

    def set_annotators(self, annotators: List[str]):
        self.default_params['annotators'] = annotators
        flag_modified(self, 'default_params')

    def set_patterns(self, patterns: List[str]):
        self.default_params['patterns'] = patterns
        flag_modified(self, 'default_params')

    def set_patterns_file(self, patterns_file: str):
        # TODO deprecate patterns_file?
        self.default_params['patterns_file'] = patterns_file
        flag_modified(self, 'default_params')

    def set_data_filenames(self, data_filenames: List[str]):
        self.default_params['data_filenames'] = data_filenames
        flag_modified(self, 'default_params')

    def get_uuid(self):
        return self.default_params.get('uuid')

    def get_labels(self):
        return self.default_params.get('labels', [])

    def get_annotators(self):
        return self.default_params.get('annotators', [])

    def get_patterns(self):
        return self.default_params.get('patterns', [])

    def get_data_filenames(self, abs=False):
        fnames = self.default_params.get('data_filenames', [])
        if abs:
            fnames = [os.path.join(filestore_base_dir(), RAW_DATA_DIR, f)
                      for f in fnames]
        return fnames

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

            _patterns = self.get_patterns()
            if _patterns is not None:
                patterns += _convert_to_spacy_patterns(_patterns)

            self.__cached_pattern_model = PatternModel(patterns)

        return self.__cached_pattern_model

    def get_active_nlp_model(self):
        # TODO refactor this to a different name later (e.g. get_latest_model)
        return self.text_classification_models.first()

    def __repr__(self):
        return "<Task with id {}, \nname {}, \ndefault_params {}>".format(
            self.id, self.name, self.default_params)


class AnnotationRequest(Base):
    __tablename__ = 'annotation_request'

    # --------- REQUIRED ---------

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Who should annotate.
    user_id = Column(Integer, ForeignKey('user.id'), nullable=False)
    user = relationship("User")

    # What Entity should the user annotate.
    entity_id = Column(Integer, ForeignKey('entity.id'), nullable=False)
    entity = relationship("Entity")

    # What Label we want the user to annotate. (Can be None)
    label_id = Column(Integer, ForeignKey('label.id'))
    label = relationship("Label")

    # TODO maybe deprecate `annotation_type` since we're already tracking label
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

    # What aspect of the Entity is presented to the user and why.
    # ** This is meant to be copied over to the Annotation **
    # Includes: text, images, probability scores, source etc.
    context = Column(JSON)


# =============================================================================
# Convenience Functions
def update_instance(dbsession, model, filter_by_dict, update_dict):
    dbsession.query(model).filter_by(**filter_by_dict).update(update_dict)
    dbsession.commit()


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

    try:
        instance = dbsession.query(model).\
            filter_by(**read_kwargs).one_or_none()
        if instance:
            return instance
        else:
            instance = model(**kwargs)
            dbsession.add(instance)
            dbsession.commit()
            logging.info("Created a new instance of {}".format(instance))
            return instance
    except Exception:
        dbsession.rollback()
        raise


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
