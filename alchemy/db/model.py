import copy
import logging
import os
import pickle
import urllib.parse
from typing import List, Optional, Union

import flask_login
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import Boolean, MetaData, UniqueConstraint, create_engine, desc, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, scoped_session, sessionmaker
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.schema import Column, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.types import JSON, DateTime, Float, Integer, String
from werkzeug.utils import secure_filename

from alchemy.db.fs import raw_data_dir, training_data_dir, filestore_base_dir
from alchemy.shared.config import Config
from alchemy.shared.utils import (
    _format_float_numbers,
    file_len,
    gen_uuid,
    load_json,
    load_jsonl,
    safe_getattr,
    stem,
)
from alchemy.train.no_deps.inference_results import InferenceResults
from alchemy.train.no_deps.metrics import compute_metrics as _compute_metrics
from alchemy.train.no_deps.paths import (
    _get_all_inference_fnames,
    _get_all_plots,
    _get_config_fname,
    _get_data_parser_fname,
    _get_exported_data_fname,
    _get_inference_fname,
    _get_metrics_fname,
    _get_metrics_v2_fname,
)
from alchemy.train.paths import get_model_dir

meta = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)
Base = declarative_base(metadata=meta)


def _convert_to_spacy_patterns(patterns: List[str]):
    return [
        {"label": "POSITIVE_CLASS", "pattern": [{"lower": x.lower()}]} for x in patterns
    ]


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

    @classmethod
    def get_all_entity_types(cls):
        return [cls.COMPANY]


class AnnotationValue:
    POSITIVE = 1
    NEGTIVE = -1
    UNSURE = 0
    NOT_ANNOTATED = -2


# A dummy entity is used to store the value of a label that don't have any real
# annotations yet, but we want to show it to the user as an option.
DUMMY_ENTITY = "__dummy__"


# =============================================================================
# Tables


class User(Base, flask_login.UserMixin):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    username = Column(String(64), index=True, unique=True, nullable=False)
    first_name = Column(String(64), index=False, unique=False, nullable=True)
    last_name = Column(String(64), index=False, unique=False, nullable=True)
    email = Column(String(128), index=False, unique=False, nullable=True)
    # A user can do many annotations.
    classification_annotations = relationship(
        "ClassificationAnnotation", back_populates="user", lazy="dynamic"
    )

    def __repr__(self):
        return "<User {}>".format(self.username)

    def get_display_name(self):
        full_name = f'{self.first_name or ""} {self.last_name or ""}'.strip()

        return full_name or self.username

    def fetch_ar_count_per_task(self):
        """Returns a list of tuples (name, task_id, count)
        """
        session = inspect(self).session

        q = (
            session.query(Task.name, Task.id, func.count(Task.id))
            .join(AnnotationRequest)
            .filter(AnnotationRequest.task_id == Task.id)
            .filter(AnnotationRequest.user == self)
            .group_by(Task.id)
            .order_by(Task.id)
        )

        return q.all()

    def fetch_ar_for_task(self, task_id, status=AnnotationRequestStatus.Pending):
        """Returns a list of AnnotationRequest objects"""
        session = inspect(self).session

        q = (
            session.query(AnnotationRequest)
            .filter(AnnotationRequest.task_id == task_id)
            .filter(AnnotationRequest.user == self)
            .filter(AnnotationRequest.status == status)
            .order_by(AnnotationRequest.order)
        )

        return q.all()


class ClassificationAnnotation(Base):
    __tablename__ = "classification_annotation"

    id = Column(Integer, primary_key=True)
    value = Column(Integer, nullable=False)
    weight = Column(Float, nullable=True, default=1.0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    entity = Column(String, index=True, nullable=False)
    entity_type = Column(String, index=True, nullable=False)

    label = Column(String, index=True, nullable=False)

    user_id = Column(Integer, ForeignKey("user.id"))
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
        Entity {},
        Entity Type {},
        Label {},
        User Id {},
        Value {},
        Created at {},
        Last Updated at {}
        """.format(
            self.id,
            self.entity,
            self.entity_type,
            self.label,
            self.user_id,
            self.value,
            self.created_at,
            self.updated_at,
        )

    @staticmethod
    def create_dummy(dbsession, entity_type, label):
        """Create a dummy record to mark the existence of a label.
        """
        return get_or_create(
            dbsession,
            ClassificationAnnotation,
            entity=DUMMY_ENTITY,
            entity_type=entity_type,
            label=label,
            value=AnnotationValue.NOT_ANNOTATED,
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

    q1 = (
        dbsession.query(
            ClassificationAnnotation.entity,
            ClassificationAnnotation.value,
            func.sum(ClassificationAnnotation.weight).label("weight"),
        )
        .filter_by(label=label)
        .filter(ClassificationAnnotation.value != AnnotationValue.UNSURE)
        .filter(ClassificationAnnotation.value != AnnotationValue.NOT_ANNOTATED)
        .group_by(ClassificationAnnotation.entity, ClassificationAnnotation.value)
    )

    q1 = q1.cte("weight_query")
    """
    q1 gives us this:
    Entity | Value | Weight
    a.com  |   1   |   50
    a.com  |   -1  |   50
    b.com  |   1   |   15
    b.com  |   -1  |   20
    c.com  |   1   |   10
    """

    q2 = dbsession.query(
        q1.c.entity,
        q1.c.value,
        q1.c.weight,
        func.row_number()
        .over(partition_by=q1.c.entity, order_by=desc(q1.c.weight))
        .label("row_number"),
    )
    q2 = q2.cte("weight_query_with_row_number")
    """
    q2 gives us this:
    Entity | Value | Weight | ROW Number
    a.com  |   1   |   50   |     1
    a.com  |   -1  |   50   |     2
    b.com  |   -1  |   20   |     1
    b.com  |   1   |   15   |     2
    c.com  |   1   |   10   1     1
    """

    query = dbsession.query(q2.c.entity, q2.c.value, q2.c.weight).filter(
        q2.c.row_number == 1
    )

    return query


class ClassificationTrainingData(Base):
    # TODO rename to BinaryTextClassificationTrainingData
    """
    This points to a jsonl file where each line is of the structure:
    {"text": "A quick brown fox", "labels": {"bear": -1}}
    """
    __tablename__ = "classification_training_data"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    label = Column(String, index=True, nullable=False)

    @staticmethod
    def create_for_label(
        dbsession, entity_type: str, label: str, entity_text_lookup_fn, batch_size=50
    ):
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
        for entity, anno_value, _ in query.yield_per(batch_size):
            looked_up_text = entity_text_lookup_fn(entity_type, entity)
            if not looked_up_text or not looked_up_text.strip():
                continue
            final.append({"text": looked_up_text, "labels": {label: anno_value}})

        # Save the database object, use it to generate filename, then save the
        # file on disk.
        data = ClassificationTrainingData(label=label)
        dbsession.add(data)
        dbsession.commit()

        output_fname = os.path.join(filestore_base_dir(), data.path())
        os.makedirs(os.path.dirname(output_fname), exist_ok=True)
        from alchemy.shared.utils import save_jsonl

        save_jsonl(output_fname, final)

        return data

    def path(self, abs=False):
        if abs:
            base = None
        else:
            base = ""

        p = os.path.join(
            training_data_dir(base),
            secure_filename(self.label),
            str(int(self.created_at.timestamp())) + ".jsonl",
        )
        return p

    def load_data(self, to_df=False):
        path = self.path(abs=True)
        return load_jsonl(path, to_df=to_df)

    def length(self):
        return file_len(self.path())


class ModelDeploymentConfig(Base):
    __tablename__ = "model_deployment_config"
    id = Column(Integer, primary_key=True)
    model_id = Column(Integer, ForeignKey("model.id"), nullable=False)
    is_approved = Column(Boolean(name="is_approved"), default=False)
    is_selected_for_deployment = Column(
        Boolean(name="is_selected_for_deployment"), default=False
    )
    threshold = Column(Float, default=0.5)

    @staticmethod
    def get_selected_for_deployment(dbsession) -> List["ModelDeploymentConfig"]:
        """Return all ModelDeploymentConfig's that are selected for deployment.
        """
        return (
            dbsession.query(ModelDeploymentConfig)
            .filter_by(is_selected_for_deployment=True)
            .all()
        )


class Model(Base):
    __tablename__ = "model"

    id = Column(Integer, primary_key=True)
    type = Column(String(64), index=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # It's useful to submit jobs from various systems to the same remote
    # training system. UUID makes sure those jobs don't clash.
    uuid = Column(String(64), index=True, nullable=False, default=gen_uuid)
    version = Column(Integer, index=True, nullable=False, default=1)

    classification_training_data_id = Column(
        Integer, ForeignKey("classification_training_data.id")
    )
    classification_training_data = relationship("ClassificationTrainingData")

    config = Column(JSON)

    # Optionally associated with a Label
    label = Column(String, index=True, nullable=True)

    entity_type = Column(
        String, index=True, nullable=True, default=EntityTypeEnum.COMPANY
    )

    __mapper_args__ = {"polymorphic_on": type, "polymorphic_identity": "model"}

    __table_args__ = (UniqueConstraint("uuid", "version", name="_uuid_version_uc"),)

    def __repr__(self):
        return f"<Model:{self.type}:{self.uuid}:{self.version}>"

    @staticmethod
    def get_latest_version(dbsession, uuid):
        res = (
            dbsession.query(Model.version)
            .filter(Model.uuid == uuid)
            .order_by(Model.version.desc())
            .first()
        )
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
        return get_model_dir(self.uuid, self.version, abs=abs)

    def inference_dir(self):
        # TODO replace with official no_deps
        return os.path.join(self.dir(), "inference")

    def _load_json(self, fname_fn):
        fname = fname_fn(self.dir(abs=True))
        if os.path.isfile(fname):
            return load_json(fname)
        else:
            return None

    def is_ready(self):
        # Model is ready when it has a metrics file.
        return self.get_metrics() is not None

    def get_metrics(self):
        return self._load_json(_get_metrics_fname)

    def get_config(self):
        return self._load_json(_get_config_fname)

    def get_data_parser(self):
        return self._load_json(_get_data_parser_fname)

    def get_url_encoded_plot_paths(self):
        """Return a list of urls for plots"""
        paths = _get_all_plots(self.dir(abs=True))
        paths = [urllib.parse.quote(x) for x in paths]
        return paths

    def get_inference_fnames(self):
        """Get the original filenames of the raw data for inference"""
        return [
            stem(path) + ".jsonl"
            for path in _get_all_inference_fnames(self.dir(abs=True))
        ]

    def export_inference(self, fname: str, include_text: bool = False):
        """Exports the given inferenced file data_fname as a dataframe.
        Returns None if the file has not been inferenced yet.
        Inputs:
            fname: The dataset filename, e.g. "spring_jan_2020.jsonl"
            include_text: True to include the "text" column.
        Returns:
            A dataframe containing columns ['name', 'domain', 'probs'] and
            'text' if include_text=True.
        """

        cols = ["name", "domain", "probs"]
        if include_text:
            cols = ["name", "domain", "text", "probs"]

        version_dir = self.dir(abs=True)
        return load_inference(version_dir, fname, columns=cols)

    def get_len_data(self):
        """Return how many datapoints were used to train this model.
        We measure the size of the file in the model directory, not to be
        confused with the file from a ClassificationTrainingData instance!
        """
        return file_len(_get_exported_data_fname(self.dir(abs=True)))

    def compute_metrics(self, threshold: float = 0.5):
        """See train.no_deps.compute_metrics"""
        version_dir = self.dir(abs=True)

        # TODO retire the old metrics.json
        metrics_path = _get_metrics_v2_fname(version_dir, threshold)

        if not os.path.isfile(metrics_path):
            cols = ["text", "probs"]

            inf_lookup = pd.DataFrame(columns=cols)

            # TODO This loads all the inferences (deduped by text) - this could
            # be prohibitively slow, is there a good place to precompute this?
            for fpath in _get_all_inference_fnames(version_dir):
                df = load_inference(version_dir, fpath, columns=cols)

                # inf_lookup can get very big, so only keep unique rows by text
                inf_lookup = pd.concat([inf_lookup, df], axis=0)
                inf_lookup = inf_lookup.drop_duplicates(subset=["text"], keep="first")

            metrics = _compute_metrics(version_dir, inf_lookup, threshold=threshold)
            pickle.dump(metrics, open(metrics_path, "wb"))

        metrics = pickle.load(open(metrics_path, "rb"))

        metrics = _reformat_metrics(metrics)

        return metrics


class TextClassificationModel(Model):
    __mapper_args__ = {"polymorphic_identity": "text_classification_model"}

    def __str__(self):
        return f"TextClassificationModel:{self.uuid}:v{self.version}"


class Task(Base):
    __tablename__ = "task"

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

    def __init__(self, *args, **kwargs):
        # Set default
        default_params = kwargs.get("default_params", {})
        if "uuid" not in default_params:
            default_params["uuid"] = gen_uuid()
        kwargs["default_params"] = default_params
        super(Task, self).__init__(*args, **kwargs)

    def __str__(self):
        return self.name

    def set_labels(self, labels: List[str]):
        self.default_params["labels"] = labels
        flag_modified(self, "default_params")

    def set_entity_type(self, entity_type: str):
        self.default_params["entity_type"] = entity_type
        flag_modified(self, "default_params")

    def set_annotators(self, annotators: List[Union[str, User]]):
        def to_username(annotator):
            if isinstance(annotator, User):
                return annotator.username
            return str(annotator)

        self.default_params["annotators"] = [to_username(a) for a in annotators]
        flag_modified(self, "default_params")

    def set_patterns(self, patterns: List[str]):
        self.default_params["patterns"] = patterns
        flag_modified(self, "default_params")

    def set_patterns_file(self, patterns_file: str):
        # TODO deprecate patterns_file?
        self.default_params["patterns_file"] = patterns_file
        flag_modified(self, "default_params")

    def set_data_filenames(self, data_filenames: List[str]):
        self.default_params["data_filenames"] = data_filenames
        flag_modified(self, "default_params")

    def get_uuid(self):
        return self.default_params.get("uuid")

    # TODO remember to write a script to backfill this field into existing
    #  task. remember to remove the default value since this is only for
    #  testing purpose.
    def get_entity_type(self):
        return self.default_params.get("entity_type", EntityTypeEnum.COMPANY)

    def get_labels(self):
        return self.default_params.get("labels", [])

    def get_annotators(self, resolve_user=False):
        annotator_list = self.default_params.get("annotators", [])
        if not resolve_user:
            return annotator_list
        logging.error(f"Annotator list = {annotator_list}")
        return db.session.query(User).filter(User.username.in_(annotator_list)).all()

    def get_patterns(self):
        return self.default_params.get("patterns", [])

    def get_data_filenames(self, abs=False):
        fnames = self.default_params.get("data_filenames", [])
        if abs:
            fnames = [_raw_data_file_path(f) for f in fnames]
        return fnames

    def get_pattern_model(self):
        from alchemy.inference.pattern_model import PatternModel

        if safe_getattr(self, "__cached_pattern_model") is None:
            patterns = []

            _patterns_file = self.default_params.get("patterns_file")
            if _patterns_file:
                patterns += load_jsonl(_raw_data_file_path(_patterns_file), to_df=False)

            _patterns = self.get_patterns()
            if _patterns is not None:
                patterns += _convert_to_spacy_patterns(_patterns)

            self.__cached_pattern_model = PatternModel(patterns)

        return self.__cached_pattern_model

    def get_annotation_link(self):
        # FIXME: SUCH A SMELLY WAY OF CREATING THE LINK!
        return f'{Config.get_annotation_server()}/tasks/{self.id}'

    def __repr__(self):
        return "<Task with id {}, \nname {}, \ndefault_params {}>".format(
            self.id, self.name, self.default_params
        )


class AnnotationRequest(Base):
    __tablename__ = "annotation_request"

    # --------- REQUIRED ---------

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    # Who should annotate.
    user_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    user = relationship("User")

    entity = Column(String, index=True, nullable=False)
    entity_type = Column(String, index=True, nullable=False)

    label = Column(String, index=True, nullable=False)

    # TODO maybe deprecate `annotation_type` since we're already tracking label
    # What kind of annotation should the user be performing.
    # See the AnnotationType enum.
    annotation_type = Column(Integer, nullable=False)

    # AnnotationRequestStatus
    status = Column(
        Integer, index=True, nullable=False, default=AnnotationRequestStatus.Pending
    )

    # --------- OPTIONAL ---------

    # Which task this request belongs to, so we can list all requests per task.
    # (If null, this request does not belong to any task)
    task_id = Column(Integer, ForeignKey("task.id"))
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


class AnnotationGuide(Base):
    __tablename__ = "annotation_guide"

    id = Column(Integer, primary_key=True)

    label = Column(String, index=True, unique=True, nullable=False)

    data = Column(JSON)

    @staticmethod
    def plaintext_to_html(plaintext):
        return "<br />".join(plaintext.split("\n"))

    def set_text(self, text):
        self.data = {"text": text, "html": AnnotationGuide.plaintext_to_html(text)}

    def get_text(self):
        if self.data and self.data.get("text"):
            return self.data.get("text")
        else:
            return ""

    def get_html(self):
        if self.data and self.data.get("html"):
            return self.data.get("html")
        else:
            return ""


class LabelOwner(Base):
    __tablename__ = "label_owner"

    id = Column(Integer, primary_key=True)

    label = Column(String, index=True, unique=True, nullable=False)

    owner_id = Column(Integer, ForeignKey("user.id"))
    owner = relationship("User")


class LabelPatterns(Base):
    __tablename__ = "label_patterns"

    id = Column(Integer, primary_key=True)

    label = Column(String, index=True, unique=True, nullable=False)

    data = Column(JSON)

    def set_positive_patterns(self, patterns):
        # Dedupe and sort the patterns
        patterns = sorted(list(set(patterns)))

        data = self.data or {}
        data.update({"positive_patterns": patterns})
        self.data = data
        flag_modified(self, "data")

    def get_positive_patterns(self):
        if self.data:
            return self.data.get("positive_patterns", [])
        else:
            return []

    def count(self):
        if self.data:
            return len(self.data.get("positive_patterns", []))
        else:
            return 0


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
        instance = dbsession.query(model).filter_by(**read_kwargs).one_or_none()
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


def fetch_labels_by_entity_type(dbsession, entity_type: str):
    """Fetch all labels for an entity type.

    :param entity_type: the entity type
    :return: all the labels under the entity type
    """
    res = (
        dbsession.query(func.distinct(ClassificationAnnotation.label))
        .filter_by(entity_type=entity_type)
        .all()
    )
    res = [x[0] for x in res]
    return res


def save_labels_by_entity_type(dbsession, entity_type: str, labels: List[str]):
    """Update labels under the entity type.

    :param entity_type: the entity type name
    :param labels: labels to be saved
    """
    # Create a dummy ClassificationAnnotation just to store the label.
    logging.info("Finding the EntityType for {}".format(entity_type))
    for label in labels:
        ClassificationAnnotation.create_dummy(dbsession, entity_type, label)


def fetch_ar_ids_by_task_and_user(dbsession, task_id, username):
    res = (
        dbsession.query(AnnotationRequest)
        .filter(AnnotationRequest.task_id == task_id, User.username == username)
        .join(User)
        .all()
    )
    return [ar.id for ar in res]


def _raw_data_file_path(fname):
    """Absolute path to a data file in the default raw data directory"""
    return os.path.join(raw_data_dir(), fname)


# TODO this does not guarantee to fetch annotations under a task. It only
#  fetch annotations with the labels under a task. If we are looking for
#  annotations created inside Alchemy website, we'd better add a source column.
def fetch_annotation_entity_and_ids_done_by_user_under_labels(
    dbsession, username, labels
):
    res = (
        dbsession.query(
            ClassificationAnnotation.entity,
            ClassificationAnnotation.id,
            ClassificationAnnotation.created_at,
            ClassificationAnnotation.label,
            ClassificationAnnotation.value,
        )
        .join(User)
        .filter(
            User.username == username,
            ClassificationAnnotation.label.in_(labels),
            ClassificationAnnotation.value != AnnotationValue.NOT_ANNOTATED,
        )
        .order_by(ClassificationAnnotation.created_at.desc())
        .all()
    )
    return res


def delete_requests_under_task_with_condition(dbsession, task_id, **kwargs):
    # Can't call Query.update() or Query.delete() when join(),
    # outerjoin(), select_from(), or from_self() has been called. So we have
    # to get the user instance first.
    if kwargs is None:
        kwargs = {"task_id": task_id}
    else:
        kwargs.update({"task_id": task_id})
        print(kwargs)

    dbsession.query(AnnotationRequest).filter_by(**kwargs).delete(
        synchronize_session=False
    )


def delete_requests_for_user_under_task(dbsession, username, task_id):
    user = dbsession.query(User).filter(User.username == username).one_or_none()
    if not user:
        logging.info("No such user {} exists. Ignored.".format(username))
        return None
    delete_requests_under_task_with_condition(
        dbsession, task_id=task_id, user_id=user.id
    )


def delete_requests_for_label_under_task(dbsession, label, task_id):
    delete_requests_under_task_with_condition(dbsession, task_id=task_id, label=label)


def delete_requests_for_entity_type_under_task(dbsession, task_id, entity_type):
    delete_requests_under_task_with_condition(
        dbsession, task_id=task_id, entity_type=entity_type
    )


def delete_requests_under_task(dbsession, task_id):
    delete_requests_under_task_with_condition(dbsession, task_id=task_id)


def get_latest_model_for_label(
    dbsession, label, model_type="text_classification_model"
):
    return (
        dbsession.query(Model)
        .filter_by(label=label, type=model_type)
        .order_by(Model.version.desc(), Model.created_at.desc())
        .first()
    )


def load_inference(
    version_dir: str, dataset_filename: str, columns: Optional[List[str]] = None
):
    """
    Inputs:
        version_dir: The model version dir
        dataset_filename: The dataset filename inference was conducted on,
            could also be the file path. E.g. "spring_jan_2020.jsonl"
        columns: A list of columns to include in the return value, by default
            ['name', 'domain', 'text', 'probs']
    Returns:
        A pandas dataframe of the result with the cols
    """
    if columns is None:
        columns = ["name", "domain", "text", "probs"]

    assert columns, "At least 1 column is required"

    # Make sure we only get the _dataset name_, in case a path was passed in
    dataset_filename = stem(dataset_filename) + ".jsonl"

    # Load Inference Results
    path = _get_inference_fname(version_dir, dataset_filename)
    ir = InferenceResults.load(path)

    # Load Original Data
    # This already includes the column "text"
    # TODO This function needs to be outside of the version_dir because the
    # original data is not in the version_dir!
    df = load_jsonl(_raw_data_file_path(dataset_filename), to_df=True)

    # Check they exist and are the same size
    assert df is not None, f"Raw data not found: {dataset_filename}"
    assert len(df) == len(ir.probs), "Inference size != Raw data size"

    # Combine the two together.
    df["probs"] = ir.probs

    if "domain" in columns:
        df["domain"] = df["meta"].apply(lambda x: x.get("domain"))

    if "name" in columns:
        df["name"] = df["meta"].apply(lambda x: x.get("name"))

    return df[columns]


def _reformat_metrics(metrics):
    logging.info("Reformatting metrics")
    train_metrics = metrics["train"]
    test_metrics = metrics["test"]

    logging.info("Train metrics are: ")
    logging.info(train_metrics)

    logging.info("Testing metrics are: ")
    logging.info(test_metrics)

    for metric in train_metrics:
        train_metrics[metric] = _format_float_numbers(train_metrics[metric])

    for metric in test_metrics:
        test_metrics[metric] = _format_float_numbers(test_metrics[metric])
    logging.info(metrics)

    return metrics
