import os
import uuid
import time
import json

from typing import List

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from db.utils import get_all_data_files
from db.task import Task
from ar.data import compute_annotation_statistics

from ar.ar_celery import generate_annotation_requests
from ar.ar_celery import app as ar_celery_app

from train.train_celery import train_model
from train.model_viewer import ModelViewer
from inference.nlp_model import NLPModel

from shared.celery_job_status import CeleryJobStatus
from shared.frontend_user_password import generate_frontend_user_login_link

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
    tasks = Task.fetch_all_tasks()
    return render_template('tasks/index.html', tasks=tasks)

@bp.route('/new', methods=['GET'])
def new():
    fnames = get_all_data_files()
    return render_template('tasks/new.html', fnames=fnames)

@bp.route('/', methods=['POST'])
def create():
    all_files = get_all_data_files()

    error = None
    try:
        form = request.form

        name = parse_name(form)
        labels = parse_labels(form)
        annotators = parse_annotators(form)
        patterns_file = parse_patterns_file(form, all_files)
        patterns = parse_patterns(form)
        data_files = parse_data(form, all_files)
    except Exception as e:
        error = str(e)
    
    if error is not None:
        flash(error)
        return render_template('tasks/new.html', fnames=all_files)
    else:
        task = Task()
        task.update_and_save(
            name=name,
            labels=labels,
            annotators=annotators,
            patterns_file=patterns_file,
            patterns=patterns,
            data_files=data_files
        )
        return redirect(url_for('tasks.show', id=task.task_id))

    return render_template('tasks/edit.html', task=task)

    

@bp.route('/<string:id>', methods=['GET'])
def show(id):
    # Basic info
    task = Task.fetch(id)

    # -------------------------------------------------------------------------
    # Annotations
    annotation_statistics = compute_annotation_statistics(task.task_id)

    status_assign_jobs_active = []
    status_assign_jobs_stale = []
    for cjs in CeleryJobStatus.fetch_all_by_context_id(f'assign:{task.task_id}'):
        if cjs.is_stale():
            status_assign_jobs_stale.append(cjs)
        else:
            status_assign_jobs_active.append(cjs)
    
    # TODO delete stale jobs on a queue, instead of here.
    for cjs in status_assign_jobs_stale:
        cjs.delete()

    # Annotator login links
    annotator_login_links = [
        (username, generate_frontend_user_login_link(username))
        for username in task.annotators
    ]

    # -------------------------------------------------------------------------
    # Models
    model_viewers: List[ModelViewer] = task.get_model_viewers()
    active_model: NLPModel = task.get_active_nlp_model()

    return render_template(
        'tasks/show.html',
        task=task,
        annotation_statistics=annotation_statistics,
        status_assign_jobs=status_assign_jobs_active,
        model_viewers=model_viewers,
        active_model=active_model,
        annotator_login_links=annotator_login_links,
    )

@bp.route('/<string:id>/edit', methods=['GET'])
def edit(id):
    task = Task.fetch(id)
    return render_template('tasks/edit.html', task=task)
    
@bp.route('/<string:id>', methods=['POST'])
def update(id):
    task = Task.fetch(id)

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
        return render_template('tasks/edit.html', task=task)
    else:
        task.update_and_save(
            name=name,
            labels=labels,
            annotators=annotators,
            patterns=patterns,
        )
        return redirect(url_for('tasks.show', id=task.task_id))


@bp.route('/<string:id>/assign', methods=['POST'])
def assign(id):
    async_result = generate_annotation_requests.delay(id, n=100, overlap=2)
    celery_id = str(async_result)
    CeleryJobStatus(celery_id, f'assign:{id}').save()
    return redirect(url_for('tasks.show', id=id))


@bp.route('/<string:id>/train', methods=['POST'])
def train(id):
    async_result = train_model.delay(id)
    # TODO
    # celery_id = str(async_result)
    # CeleryJobStatus(celery_id, f'assign:{id}').save()
    return redirect(url_for('tasks.show', id=id))


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
        labels = Task.parse_jinjafied('labels', labels)
    except Exception as e:
        raise Exception(f'Unable to load Labels: {e}')
    
    assert isinstance(labels, list), 'Labels must be a list'
    assert len(labels) > 0, 'Labels must not be empty'
    return labels

def parse_annotators(form):
    annotators = form['annotators']
    assert annotators, 'Annotators is required'

    try:
        annotators = Task.parse_jinjafied('annotators', annotators)
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
    assert isinstance(selections, list), "Pattern file selections is not a list"

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
        patterns = Task.parse_jinjafied('patterns', patterns)
    except Exception as e:
        raise Exception(f'Unable to load Patterns: {e}')

    assert isinstance(patterns, list), 'Patterns must be a list'
    return patterns
