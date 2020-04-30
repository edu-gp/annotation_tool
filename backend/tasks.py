from typing import List

from flask import (
    Blueprint, flash, redirect, render_template, request, url_for
)

from db.model import (
    db, Task, Model, TextClassificationModel, FileInference
)
from db.utils import get_all_data_files, get_all_pattern_files
from ar.data import compute_annotation_statistics, \
    compute_annotation_statistics_db, compute_annotation_request_statistics

from ar.ar_celery import generate_annotation_requests

from train.train_celery import train_model as local_train_model
from train.gcp_celery import poll_status as gcp_poll_status
from train.no_deps.utils import get_env_bool

from shared.celery_job_status import (
    CeleryJobStatus, create_status, delete_status
)
from shared.frontend_user_password import generate_frontend_user_login_link
from shared.utils import (
    get_env_int, stem, list_to_textarea, textarea_to_list,
)

from .auth import auth

bp = Blueprint('tasks', __name__, url_prefix='/tasks')


@auth.login_required
def _before_request():
    print("Before Request")
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route('/')
def index():
    tasks = db.session.query(Task).order_by(Task.created_at.desc()).all()
    return render_template('tasks/index.html', tasks=tasks)


@bp.route('/new', methods=['GET'])
def new():
    data_fnames = get_all_data_files()
    pattern_fnames = get_all_pattern_files()
    return render_template('tasks/new.html',
                           data_fnames=data_fnames,
                           pattern_fnames=pattern_fnames)


@bp.route('/', methods=['POST'])
def create():
    data_fnames = get_all_data_files()
    pattern_fnames = get_all_pattern_files()

    error = None
    try:
        form = request.form

        name = parse_name(form)
        labels = parse_labels(form)
        annotators = parse_annotators(form)
        patterns_file = parse_patterns_file(form, pattern_fnames)
        patterns = parse_patterns(form)
        data_files = parse_data(form, data_fnames)
    except Exception as e:
        error = str(e)

    if error is not None:
        flash(error)
        return render_template('tasks/new.html',
                               data_fnames=data_fnames,
                               pattern_fnames=pattern_fnames)
    else:
        task = Task(name=name)
        task.set_labels(labels)
        task.set_annotators(annotators)
        task.set_patterns_file(patterns_file)
        task.set_patterns(patterns)
        task.set_data_filenames(data_files)
        db.session.add(task)
        db.session.commit()
        return redirect(url_for('tasks.show', id=task.id))


@bp.route('/<string:id>', methods=['GET'])
def show(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    # -------------------------------------------------------------------------
    # Annotations
    annotation_statistics_per_label = dict()
    for label in task.get_labels():
        annotation_statistics_per_label[label] = \
            compute_annotation_statistics_db(dbsession=db.session,
                                             label_name=label)

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

    # -------------------------------------------------------------------------
    # Models
    models = task.text_classification_models.all()
    active_model: Model = task.get_active_nlp_model()

    return render_template(
        'tasks/show.html',
        task=task,
        annotation_statistics_per_label=annotation_statistics_per_label,
        annotation_request_statistics=annotation_request_statistics,
        status_assign_jobs=status_assign_jobs_active,
        models=models,
        active_model=active_model,
        annotator_login_links=annotator_login_links,
    )


@bp.route('/<string:id>/edit', methods=['GET'])
def edit(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()
    return render_template('tasks/edit.html', task=task,
                           list_to_textarea=list_to_textarea)


@bp.route('/<string:id>', methods=['POST'])
def update(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    error = None
    try:
        form = request.form

        name = parse_name(form)
        labels = parse_labels(form)
        annotators = parse_annotators(form)
        patterns = parse_patterns(form)
    except Exception as e:
        error = str(e)

    if error is not None:
        flash(error)
        return render_template('tasks/edit.html', task=task,
                               list_to_textarea=list_to_textarea)
    else:
        task.name = name
        task.set_labels(labels)
        task.set_annotators(annotators)
        task.set_patterns(patterns)
        db.session.add(task)
        db.session.commit()
        return redirect(url_for('tasks.show', id=task.id))


@bp.route('/<string:id>/assign', methods=['POST'])
def assign(id):
    max_per_annotator = get_env_int('ANNOTATION_TOOL_MAX_PER_ANNOTATOR', 100)
    max_per_dp = get_env_int('ANNOTATION_TOOL_MAX_PER_DP', 3)
    async_result = generate_annotation_requests.delay(
        id, max_per_annotator=max_per_annotator, max_per_dp=max_per_dp)
    celery_id = str(async_result)
    create_status(celery_id, f'assign:{id}')
    return redirect(url_for('tasks.show', id=id))


@bp.route('/<string:id>/train', methods=['POST'])
def train(id):
    if get_env_bool('GOOGLE_AI_PLATFORM_ENABLED', False):
        # TODO use celery to set this up - last I tried there were some issues
        from train.prep import prepare_task_for_training
        from train.gcp_job import GCPJob

        task = db.session.query(Task).filter_by(id=id).one_or_none()

        model = prepare_task_for_training(db.session, task.id)

        files_for_inference = task.get_data_filenames(abs=True)

        job = GCPJob(model.uuid, model.version)
        # TODO: A duplicate job would error out.
        job.submit(files_for_inference)

        async_result = gcp_poll_status.delay(model.id)
    else:
        async_result = local_train_model.delay(id)
    # TODO
    # celery_id = str(async_result)
    # CeleryJobStatus(celery_id, f'assign:{id}').save()
    return redirect(url_for('tasks.show', id=id))


@bp.route('/<string:id>/download_prediction', methods=['POST'])
def download_prediction(id):
    task = db.session.query(Task).filter_by(id=id).one_or_none()

    model_id = int(request.form['model_id'])
    fname = request.form['fname']

    inf = db.session.query(FileInference).filter_by(
        model_id=model_id, input_filename=fname).one_or_none()

    if task is not None and inf is not None:
        df = inf.create_exported_dataframe()

        # Write it to a temp file and send it.
        import tempfile
        import os
        from werkzeug.utils import secure_filename
        from flask import send_file

        with tempfile.TemporaryDirectory() as tmpdirname:
            name = f"{secure_filename(task.name)}__{stem(fname)}.csv"
            final_fname = os.path.join(tmpdirname, name)
            df.to_csv(final_fname, index=False)
            return send_file(final_fname, mimetype='text/csv', cache_timeout=0,
                             as_attachment=True)
    else:
        return "Task or Inference file not found", 404


# ----- FORM PARSING -----

def parse_name(form):
    name = form['name']
    name = name.strip()
    assert name, 'Name is required'
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


def parse_patterns_file(form, all_files):
    selections = form.getlist('patterns_file')
    assert isinstance(
        selections, list), "Pattern file selections is not a list"

    if len(selections) > 0:
        assert len(selections) == 1, "Only 1 pattern file should be selected"
        patterns_file = selections[0]
        assert patterns_file in all_files, "Pattern file does not exist"
        return patterns_file
    else:
        # Note a patterns file is optional
        return None


def parse_patterns(form):
    patterns = form['patterns']

    try:
        patterns = textarea_to_list(patterns)
    except Exception as e:
        raise Exception(f'Unable to load Patterns: {e}')

    assert isinstance(patterns, list), 'Patterns must be a list'
    return patterns
