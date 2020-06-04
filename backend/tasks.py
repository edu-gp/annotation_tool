import logging
from typing import List, Optional

from flask import (
    Blueprint, flash, redirect, render_template, request, url_for
)
from sqlalchemy.exc import DatabaseError
from werkzeug.urls import url_encode

from db.model import db, Task, Model, AnnotationGuide, LabelPatterns, \
    AnnotationRequest, User, delete_requests_for_user_under_task
from db.utils import get_all_data_files
from ar.data import compute_annotation_statistics_db, \
    compute_annotation_request_statistics

from ar.ar_celery import generate_annotation_requests

from train.train_celery import (
    train_model as local_train_model,
    submit_gcp_training
)
from train.no_deps.utils import get_env_bool

from shared.celery_job_status import (
    CeleryJobStatus, create_status, delete_status
)
from shared.frontend_path_finder import generate_frontend_user_login_link, \
    generate_frontend_admin_examine_link, generate_frontend_compare_link
from shared.utils import (
    get_env_int, stem, list_to_textarea, textarea_to_list,
)

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
    return render_template('tasks/new.html', data_fnames=get_all_data_files())


@bp.route('/', methods=['POST'])
def create():
    data_fnames = get_all_data_files()

    try:
        form = request.form

        name = parse_name(form)
        labels = parse_labels(form)
        annotators = parse_annotators(form)
        data_files = parse_data(form, data_fnames)

        task = Task(name=name)
        task.set_labels(labels)
        task.set_annotators(annotators)
        task.set_data_filenames(data_files)

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
    for label in task.get_labels():
        models = db.session.query(Model).filter_by(
            label=label).order_by(Model.created_at.desc()).limit(10).all()
        models_per_label[label] = models

    return render_template(
        'tasks/show.html',
        task=task,
        annotation_statistics_per_label=annotation_statistics_per_label,
        annotation_request_statistics=annotation_request_statistics,
        status_assign_jobs=status_assign_jobs_active,
        models_per_label=models_per_label,
        annotator_login_links=annotator_login_links,
        admin_examine_links=admin_examine_links,
        labels_and_attributes=labels_and_attributes,
        kappa_analysis_for_all_users_links=kappa_analysis_for_all_users_links
    )


@bp.route('/<string:id>/edit', methods=['GET'])
def edit(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()
    return render_template('tasks/edit.html', task=task,
                           list_to_textarea=list_to_textarea)


@bp.route('/<string:id>', methods=['POST'])
def update(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    try:
        form = request.form

        name = parse_name(form)
        data = parse_data_filename(form)
        labels = parse_labels(form)
        annotators = parse_annotators(form)

        task.name = name
        task.set_data_filenames([data])
        task.set_labels(labels)

        for current_annotator in task.get_annotators():
            if current_annotator not in annotators:
                logging.info("Prepare to remove requests under user {} for "
                              "task {}".format(current_annotator, id))
                delete_requests_for_user_under_task(db.session,
                                                    current_annotator,
                                                    id)
        task.set_annotators(annotators)

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
    logging.error("generating annotations asynchronously.")
    async_result = generate_annotation_requests.delay(
        id, max_per_annotator=max_per_annotator, max_per_dp=max_per_dp)
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
        async_result = submit_gcp_training.delay(label, raw_file_path)
    else:
        async_result = local_train_model.delay(label, raw_file_path)
    # TODO
    # celery_id = str(async_result)
    # CeleryJobStatus(celery_id, f'assign:{id}').save()
    return redirect(url_for('tasks.show', id=id))


@bp.route('/download_prediction', methods=['POST'])
def download_prediction():
    model_id = int(request.form['model_id'])
    fname = request.form['fname']

    model = db.session.query(Model).filter_by(id=model_id).one_or_none()

    if model is not None:
        # --- 1. Get the model inference file ---
        label = model.label or "UNK_LABEL"
        df = model.export_inference(fname, include_text=True)

        # --- 2. Merge it with the existing annotations from all users ---
        # This makes it easier to QA the model.
        from db.model import ClassificationAnnotation, User, EntityTypeEnum
        import pandas as pd
        q = db.session.query(
            User.username,
            ClassificationAnnotation.entity,
            ClassificationAnnotation.value
        ).join(User).filter(
            ClassificationAnnotation.label == label,
            ClassificationAnnotation.entity_type == EntityTypeEnum.COMPANY)
        all_annos = q.all()

        # Convert query result into a dataframe
        df_all_annos = pd.DataFrame(
            all_annos, columns=['username', 'entity', 'value'])

        # Make sure the annotations are uniquer on (user, entity)
        df_all_annos = df_all_annos.drop_duplicates(
            ['username', 'entity'], keep='first')

        # Make sure none of the entities are missing
        # (otherwise this will result in extra rows when merging)
        df_all_annos = df_all_annos.dropna(subset=['entity'])

        # Merge it with the existing annotation one by one
        n_cols = len(df.columns)
        for username in df_all_annos['username'].drop_duplicates():
            # Get just this user's annotations.
            _df = df_all_annos[df_all_annos['username'] == username]
            # Rename the "value" column to the username.
            _df = _df.drop(columns=['username'])
            _df = _df.rename(columns={'value': username, 'entity': 'domain'})
            # Merge it with the main dataframe.
            df = df.merge(_df, on='domain', how='left')

        # Compute some statistics of the annotations
        from collections import Counter
        import numpy as np

        def _build_counter(annos: List[Optional[int]]):
            """
            Input is a list of annotation values \in {-1, 0, 1, nan}.
            We ignore 0 and nan, and return a Counter of {-1, 1}.
            """
            # Ignore all the elements that are 0 or nan.
            annos = [x for x in annos if x != 0 and not pd.isna(x)]
            return Counter(annos)

        def _get_entropy(annos: List[Optional[int]], eps=0.0001):
            """Contentiousness measured by entropy"""
            cnt = _build_counter(annos)

            total = sum(cnt.values()) + eps
            probs = [cnt[x]/total for x in cnt]
            log_probs = [np.log(p + eps) for p in probs]
            entropy = -sum([p*logp for p, logp in zip(probs, log_probs)])
            return entropy

        def _get_majority_vote(annos: List[Optional[int]]):
            cnt = _build_counter(annos)

            if len(cnt):
                return cnt.most_common(1)[0][0]
            else:
                return None

        # Only consider the columns with the user annotations
        df_annos = df.iloc[:, n_cols:]
        df['CONTENTION (ENTROPY)'] = df_annos.apply(_get_entropy, axis=1)
        df['MAJORITY_VOTE'] = df_annos.apply(_get_majority_vote, axis=1)

        # 3. --- Write it to a temp file and send it ---
        import tempfile
        import os
        from werkzeug.utils import secure_filename
        from flask import send_file

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
