import os
import logging
import tempfile
import pandas as pd

from flask import (
    Blueprint, flash, redirect, render_template, request, url_for, send_file
)
from werkzeug.utils import secure_filename

from db.model import (
    db, ClassificationAnnotation, User, Task, Model, AnnotationGuide,
    LabelPatterns, ModelDeploymentConfig, EntityTypeEnum,
    delete_requests_for_user_under_task, delete_requests_for_label_under_task,
    delete_requests_under_task, delete_requests_for_entity_type_under_task
)
from db.utils import get_all_data_files

from ar.data import (
    compute_annotation_statistics_db, compute_annotation_request_statistics
)
from ar.ar_celery import generate_annotation_requests

from train.train_celery import (
    train_model as local_train_model, submit_gcp_training
)
from train.no_deps.utils import get_env_bool

from shared.celery_job_status import (
    CeleryJobStatus, create_status, delete_status
)
from shared.frontend_path_finder import (
    generate_frontend_user_login_link, generate_frontend_admin_examine_link,
    generate_frontend_compare_link
)
from shared.utils import (
    get_env_int, stem, list_to_textarea, textarea_to_list, get_entropy,
    get_majority_vote,
    get_majority_vote_v2)

from .auth import auth

bp = Blueprint('tasks', __name__, url_prefix='/tasks')


@auth.login_required
def _before_request():
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route('/')
def index():
    tasks = db.session.query(Task).order_by(Task.created_at.desc()).all()
    return render_template('tasks/index.html', tasks=tasks)


@bp.route('/new', methods=['GET'])
def new():
    return render_template('tasks/new.html', data_fnames=get_all_data_files(),
                           entity_types=EntityTypeEnum.get_all_entity_types())


@bp.route('/', methods=['POST'])
def create():
    data_fnames = get_all_data_files()

    try:
        form = request.form

        name = parse_name(form)
        entity_type = parse_entity_type(form)
        labels = parse_labels(form)
        annotators = parse_annotators(form)
        data_files = parse_data(form, data_fnames)

        task = Task(name=name)
        task.set_labels(labels)
        task.set_annotators(annotators)
        task.set_data_filenames(data_files)
        task.set_entity_type(entity_type)

        db.session.add(task)
        db.session.commit()
        return redirect(url_for('tasks.show', id=task.id))
    except Exception as e:
        db.session.rollback()
        error = str(e)
        flash(error)
        return render_template('tasks/new.html', data_fnames=data_fnames)


@bp.route('/<string:id>', methods=['GET'])
def show(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    # -------------------------------------------------------------------------
    # Labels

    _labels = task.get_labels()
    _guides = db.session.query(AnnotationGuide).filter(
        AnnotationGuide.label.in_(_labels)).all()
    _label_patterns = db.session.query(LabelPatterns).filter(
        LabelPatterns.label.in_(_labels)).all()
    # Not all labels have an AnnotationGuide or LabelPatterns, so we use this
    # _lookup to only keep the ones with a guide.
    _lookup_g = dict([(x.label, x) for x in _guides])
    _lookup_p = dict([(x.label, x) for x in _label_patterns])

    labels_and_attributes = [(label, _lookup_g.get(label), _lookup_p.get(label))
                             for label in _labels]
    labels_and_attributes = sorted(labels_and_attributes, key=lambda x: x[0])

    # -------------------------------------------------------------------------
    # Annotations
    annotation_statistics_per_label = dict()
    for label in task.get_labels():
        annotation_statistics_per_label[label] = \
            compute_annotation_statistics_db(dbsession=db.session,
                                             label=label, task_id=id)

    annotation_request_statistics = compute_annotation_request_statistics(
        dbsession=db.session, task_id=id)
    # annotation_statistics = compute_annotation_statistics(task.task_id)

    status_assign_jobs_active = []
    status_assign_jobs_stale = []
    for cjs in CeleryJobStatus.fetch_all_by_context_id(f'assign:{task.id}'):
        if cjs.is_stale():
            status_assign_jobs_stale.append(cjs)
        else:
            status_assign_jobs_active.append(cjs)

    # TODO delete stale jobs on a queue, instead of here.
    for cjs in status_assign_jobs_stale:
        delete_status(cjs.celery_id, cjs.context_id)

    # Annotator login links
    annotator_login_links = [
        (username, generate_frontend_user_login_link(username))
        for username in task.get_annotators()
    ]

    # Admin Examine Links
    admin_examine_links = [
        (username, generate_frontend_admin_examine_link(id, username))
        for username in task.get_annotators()
    ]

    kappa_analysis_for_all_users_links = {
        label: generate_frontend_compare_link(id, label)
        for label in task.get_labels()
    }

    # -------------------------------------------------------------------------
    # Models

    # TODO optimize query
    models_per_label = {}
    deployment_configs_per_model = {}
    for label in task.get_labels():
        models = db.session.query(Model).filter_by(
            label=label).order_by(Model.created_at.desc()).limit(10).all()
        models_per_label[label] = models
        model_ids = [model.id for model in models]
        res = db.session.query(
            ModelDeploymentConfig.model_id,
            ModelDeploymentConfig.is_approved,
            ModelDeploymentConfig.is_selected_for_deployment,
            ModelDeploymentConfig.threshold).\
            filter(ModelDeploymentConfig.model_id.in_(model_ids)).all()
        for model_id, is_approved, is_selected_for_deployment, threshold in \
                res:
            deployment_configs_per_model[model_id] = {
                "is_approved": is_approved,
                "is_selected_for_deployment": is_selected_for_deployment,
                "threshold": threshold
            }

    return render_template(
        'tasks/show.html',
        task=task,
        annotation_statistics_per_label=annotation_statistics_per_label,
        annotation_request_statistics=annotation_request_statistics,
        status_assign_jobs=status_assign_jobs_active,
        models_per_label=models_per_label,
        deployment_configs_per_model=deployment_configs_per_model,
        annotator_login_links=annotator_login_links,
        admin_examine_links=admin_examine_links,
        labels_and_attributes=labels_and_attributes,
        kappa_analysis_for_all_users_links=kappa_analysis_for_all_users_links
    )


@bp.route('/<string:id>/edit', methods=['GET'])
def edit(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()
    return render_template('tasks/edit.html', task=task,
                           list_to_textarea=list_to_textarea,
                           entity_types=EntityTypeEnum.get_all_entity_types())


@bp.route('/<string:id>', methods=['POST'])
def update(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    try:
        form = request.form

        name = parse_name(form)
        data = parse_data_filename(form)
        labels = parse_labels(form)

        annotators = parse_annotators(form)
        entity_type = task.get_entity_type()


        _remove_obsolete_requests_under_task(task, data,
                                             annotators, labels, entity_type)


        task.set_data_filenames([data])
        task.set_annotators(annotators)
        task.set_labels(labels)
        task.name = name


        db.session.add(task)

        db.session.commit()
        logging.info("Updated tasks and deleted requests from removed "
                     "annotators.")
        return redirect(url_for('tasks.show', id=task.id))
    except Exception as e:
        db.session.rollback()
        error = str(e)
        flash(error)
        return render_template('tasks/edit.html', task=task,
                               list_to_textarea=list_to_textarea)


@bp.route('/<string:id>/assign', methods=['POST'])
def assign(id):
    max_per_annotator = get_env_int('ANNOTATION_TOOL_MAX_PER_ANNOTATOR', 100)
    max_per_dp = get_env_int('ANNOTATION_TOOL_MAX_PER_DP', 3)
    entity_type = request.form.get('entity_type')
    if entity_type is None:
        msg = f"Cannot request annotations without " \
              f"an entity type for task {id}."
        logging.error(msg)
        raise ValueError(msg)
    logging.info("generating annotations asynchronously.")
    async_result = generate_annotation_requests.delay(
        task_id=id, max_per_annotator=max_per_annotator,
        max_per_dp=max_per_dp, entity_type=entity_type
    )
    celery_id = str(async_result)
    # Touching Redis, no need to change anything.
    create_status(celery_id, f'assign:{id}')
    return redirect(url_for('tasks.show', id=id))


@bp.route('/<string:id>/train', methods=['POST'])
def train(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    label = request.form['label']
    assert label in task.get_labels(), \
        f'Label "{label}" does not belong to task {task.id}'

    raw_file_path = task.get_data_filenames(abs=True)[0]

    if get_env_bool('GOOGLE_AI_PLATFORM_ENABLED', False):
        async_result = submit_gcp_training.delay(
            label, raw_file_path, entity_type=task.get_entity_type())
    else:
        async_result = local_train_model.delay(
            label, raw_file_path, entity_type=task.get_entity_type())
    # TODO
    # celery_id = str(async_result)
    # CeleryJobStatus(celery_id, f'assign:{id}').save()
    return redirect(url_for('tasks.show', id=id))


@bp.route('/download_training_data', methods=['POST'])
def download_training_data():
    model_id = int(request.form['model_id'])
    model = db.session.query(Model).filter_by(id=model_id).one_or_none()
    fname = model.classification_training_data.path(abs=True)
    return send_file(fname, mimetype='text/csv', cache_timeout=0,
                     as_attachment=True)


@bp.route('/download_prediction', methods=['POST'])
def download_prediction():
    model_id = int(request.form['model_id'])
    fname = request.form['fname']
    entity_type = request.form['entity_type']

    model = db.session.query(Model).filter_by(id=model_id).one_or_none()

    if model is not None:
        # --- 1. Get the model inference file ---
        label = model.label or "UNK_LABEL"
        df = model.export_inference(fname, include_text=True)

        # TODO need to update the code to get the weight and re-calculate the
        #  majority vote.

        # --- 2. Merge it with the existing annotations from all users ---
        # This makes it easier to QA the model.
        q = db.session.query(
            User.username,
            ClassificationAnnotation.entity,
            ClassificationAnnotation.value,
            ClassificationAnnotation.weight
        ).join(User).filter(
            ClassificationAnnotation.label == label,
            ClassificationAnnotation.entity_type == entity_type)
        all_annos = q.all()

        # Convert query result into a dataframe
        df_all_annos = pd.DataFrame(
            all_annos, columns=['username', 'entity', 'value', 'weight'])

        # Make sure the annotations are unique on (user, entity)
        df_all_annos = df_all_annos.drop_duplicates(
            ['username', 'entity'], keep='first')

        # Make sure none of the entities are missing
        # (otherwise this will result in extra rows when merging)
        df_all_annos = df_all_annos.dropna(subset=['entity'])
        # Merge it with the existing annotation one by one
        n_cols = len(df.columns)
        usernames = df_all_annos['username'].drop_duplicates().values
        for username in usernames:
            # Get just this user's annotations.
            _df = df_all_annos[df_all_annos['username'] == username]
            # Rename the "value" column to the username.
            _df = _df.drop(columns=['username'])
            _df = _df.rename(columns={'value': username,
                                      'entity': 'domain',
                                      'weight': username+"_vote_weight"})
            # Merge it with the main dataframe.
            df = df.merge(_df, on='domain', how='left')

        # Compute some statistics of the annotations
        # Only consider the columns with the user annotations
        # df_annos = df.iloc[:, n_cols:]
        df_annos = df[usernames]
        df['CONTENTION (ENTROPY)'] = df_annos.apply(get_entropy, axis=1)
        df['MAJORITY_VOTE'] = df_annos.apply(get_majority_vote_v2, axis=1)

        # 3. --- Write it to a temp file and send it ---
        with tempfile.TemporaryDirectory() as tmpdirname:
            name = f"{secure_filename(label)}__{stem(fname)}.csv"
            final_fname = os.path.join(tmpdirname, name)
            df.to_csv(final_fname, index=False)
            return send_file(final_fname, mimetype='text/csv', cache_timeout=0,
                             as_attachment=True)
    else:
        return "Inference file not found", 404


# ----- FORM PARSING -----

def parse_name(form):
    name = form['name']
    name = name.strip()
    assert name, 'Name is required'
    return name


def parse_data_filename(form):
    name = form['data']
    name = name.strip()
    assert name, 'Data is required'
    return name


def parse_entity_type(form):
    entity_type = form['entity_type']
    entity_type = entity_type.strip()
    assert entity_type, 'Entity type is required'
    return entity_type


def parse_labels(form):
    labels = form['labels']
    assert labels, 'Labels is required'

    try:
        labels = textarea_to_list(labels)
    except Exception as e:
        raise Exception(f'Unable to load Labels: {e}')

    assert isinstance(labels, list), 'Labels must be a list'
    assert len(labels) > 0, 'Labels must not be empty'
    return labels


def parse_annotators(form):
    annotators = form['annotators']
    assert annotators, 'Annotators is required'

    try:
        annotators = textarea_to_list(annotators)
    except Exception as e:
        raise Exception(f'Unable to load Annotators: {e}')

    assert isinstance(annotators, list), 'Annotators must be a list'
    assert len(annotators) > 0, 'Annotators must not be empty'
    return annotators


def parse_data(form, all_files):
    data = form.getlist('data')
    assert data, "Data is required"
    assert isinstance(data, list), "Data is not a list"
    assert len(data) > 0, "At least 1 data file must be selected"
    for fname in data:
        assert fname in all_files, f"Data file '{fname}' does not exist"
    return data


def _remove_obsolete_requests_under_task(task, data, annotators,
                                         labels, entity_type):
    if data != task.get_data_filenames()[0]:
        logging.info("Prepare to remove all requests under task {} "
                     "since the data file has changed".format(task.id))
        delete_requests_under_task(db.session, task.id)
    elif entity_type != task.get_entity_type():
        logging.info("Prepare to remove all requests under task {} "
                     "since the entity type has changed".format(task.id))
        delete_requests_for_entity_type_under_task(db.session, task.id,
                                                   entity_type)
    else:
        # Updating the annotators
        for current_annotator in task.get_annotators():
            if current_annotator not in annotators:
                logging.info("Prepare to remove requests under user {} for "
                             "task {}".format(current_annotator, task.id))
                delete_requests_for_user_under_task(db.session,
                                                    current_annotator,
                                                    task.id)
        # Updating the labels
        for current_label in task.get_labels():
            if current_label not in labels:
                logging.info("Prepare to remove requests under label {} for "
                             "task {}".format(current_label, task.id))
                delete_requests_for_label_under_task(db.session,
                                                     current_label, task.id)
