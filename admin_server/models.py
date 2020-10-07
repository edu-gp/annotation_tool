from dataclasses import dataclass
from typing import List

from flask import render_template
from sqlalchemy import func, desc
import logging

from flask import (
    Blueprint, request, jsonify,
    redirect)
from sqlalchemy.exc import DatabaseError

from bg.jobs import export_new_raw_data as _export_new_raw_data
from db.model import db, Model, ModelDeploymentConfig, Task, \
    ClassificationAnnotation, User, AnnotationValue, LabelOwner

bp = Blueprint('models', __name__, url_prefix='/models')

# TODO API auth


@dataclass
class ModelDataRow:
    label: str
    latest_version: int
    deployed_version: int
    num_of_data_points: int
    threshold: float
    majority_annotator: str
    has_deployed: bool
    owner_id: int
    roc_auc: float = None
    pr: list = None
    rc: list = None
    f1: list = None


def get_request_data():
    """Returns a dict of request keys and values, from either json or form"""
    if len(request.form) > 0:
        return request.form
    else:
        return request.get_json()


@bp.route('export_new_raw_data', methods=['POST'])
def export_new_raw_data():
    data = get_request_data()

    model_id = int(data.get('model_id'))
    data_fname = data.get('data_fname')
    output_fname = data.get('output_fname')
    cutoff = float(data.get('cutoff'))

    resp = {
        'error': None,
        'message': 'Request not processed.'
    }

    try:
        assert data_fname is not None, f"Missing data_fname"
        assert output_fname is not None, f"Missing output_fname"
        assert 0.0 <= cutoff < 1.0, f"Invalid cutoff={cutoff}"

        output_fname = output_fname.replace(' ', '_')

        model = db.session.query(Model).filter_by(id=model_id).one_or_none()
        output_path = _export_new_raw_data(
            model, data_fname, output_fname, cutoff=cutoff)
        resp['message'] = f'Successfully created raw data: {output_path}'
    except Exception as e:
        resp['error'] = str(e)
        resp['message'] = f"Error: {resp['error']}"

    return jsonify(resp)


@bp.route('update_model_deployment_config', methods=['POST'])
def update_model_deployment_config():
    approved_model_ids = set(request.form.getlist("approved_model_id"))
    selected_model_id_for_deployment = request.form.get("selected_model_id")

    label = request.form.get("label")

    models = db.session.query(Model).filter(Model.label == label).all()

    for model in models:
        threshold = request.form.get(str(model.id) + "_threshold", None)
        if threshold:
            threshold = float(threshold)
        model_deployment_config = db.session.query(ModelDeploymentConfig).\
            filter(ModelDeploymentConfig.model_id == model.id).one_or_none()
        is_approved = str(model.id) in approved_model_ids
        is_selected_for_deployment = str(model.id) == selected_model_id_for_deployment
        if model_deployment_config:
            model_deployment_config.is_approved = is_approved
            model_deployment_config.is_selected_for_deployment = is_selected_for_deployment
            model_deployment_config.threshold = threshold
        else:
            model_deployment_config = ModelDeploymentConfig(
                model_id=model.id,
                is_approved=is_approved,
                is_selected_for_deployment=is_selected_for_deployment,
                threshold=threshold
            )
        db.session.add(model_deployment_config)

    try:
        db.session.commit()
    except DatabaseError as e:
        db.session.rollback()
        logging.error(e)
        raise

    logging.info(f"Updated model deployment config for label {label}.")

    return redirect(request.referrer)


@bp.route('/', methods=['GET'])
def index():
    data_row_per_label = _collect_model_data_rows()
    users = db.session.query(User.id, User.username).all()
    return render_template('models/index.html',
                           data_rows=data_row_per_label,
                           users=users)


@bp.route('show/<string:label>', methods=['GET'])
def show(label):
    models_per_label = {}
    deployment_configs_per_model = {}
    models = db.session.query(Model).filter_by(
        label=label).order_by(Model.created_at.desc()).limit(10).all()
    models_per_label[label] = models
    model_ids = [model.id for model in models]
    res = db.session.query(
        ModelDeploymentConfig.model_id,
        ModelDeploymentConfig.is_approved,
        ModelDeploymentConfig.is_selected_for_deployment,
        ModelDeploymentConfig.threshold). \
        filter(ModelDeploymentConfig.model_id.in_(model_ids)).all()
    for model_id, is_approved, is_selected_for_deployment, threshold in \
            res:
        deployment_configs_per_model[model_id] = {
            "is_approved": is_approved,
            "is_selected_for_deployment": is_selected_for_deployment,
            "threshold": threshold
        }

    return render_template(
        'models/show.html',
        models_per_label=models_per_label,
        deployment_configs_per_model=deployment_configs_per_model,
        label=label
    )


def _collect_model_data_rows():
    data_row_per_label = []

    tasks = db.session.query(Task).all()
    labels = []
    for task in tasks:
        labels.extend(task.get_labels())
    for label in set(labels):
        deployed_model = db.session.query(Model).join(ModelDeploymentConfig). \
            filter(Model.label == label,
                   ModelDeploymentConfig.is_selected_for_deployment == True,
                   ModelDeploymentConfig.model_id == Model.id).one_or_none()
        latest_model = db.session.query(Model). \
            filter(Model.label == label).order_by(Model.created_at.desc()). \
            first()
        chosen_model = deployed_model if deployed_model else latest_model

        threshold = None
        if chosen_model:
            deployment_config = db.session.query(ModelDeploymentConfig).\
                filter(ModelDeploymentConfig.model_id == chosen_model.id).\
                one_or_none()
            if deployment_config:
                threshold = deployment_config.threshold

        annotators_sorted = db.session.query(User.username,
                               func.count(ClassificationAnnotation.id).label(
                                   'num')). \
            join(ClassificationAnnotation). \
            filter(ClassificationAnnotation.label == label,
                   ClassificationAnnotation.value != AnnotationValue.NOT_ANNOTATED). \
            group_by(User.username).order_by(desc('num')).all()
        if annotators_sorted and len(annotators_sorted) > 0:
            majority_annotator = annotators_sorted[0][0]
        else:
            majority_annotator = None

        label_owner_id = db.session.query(LabelOwner.owner_id).filter(
            LabelOwner.label == label).one_or_none()

        row = ModelDataRow(
            label=label,
            latest_version=latest_model.version if latest_model else None,
            deployed_version=deployed_model.version if deployed_model else None,
            num_of_data_points=chosen_model.get_len_data() if chosen_model else None,
            threshold=threshold,
            majority_annotator=majority_annotator,
            has_deployed=True if deployed_model else False,
            owner_id=label_owner_id[0] if label_owner_id else -1
        )

        if chosen_model and chosen_model.is_ready():
            test_metrics = chosen_model.get_metrics().get('test', {})

            row.roc_auc = _reformat_stats(test_metrics.get('roc_auc', None))
            row.pr = _reformat_stats(test_metrics.get('precision', None))
            row.rc = _reformat_stats(test_metrics.get('recall', None))
            row.f1 = _reformat_stats(test_metrics.get('fscore', None))

        data_row_per_label.append(row)
    return data_row_per_label


def _reformat_stats(stat: float or List):
    if isinstance(stat, float):
        stat = float("{:10.2f}".format(stat))
    elif isinstance(stat, List):
        for i in range(len(stat)):
            stat[i] = float("{:10.2f}".format(stat[i]))
    return stat
