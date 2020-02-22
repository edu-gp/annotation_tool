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
from ar.data import fetch_all_annotations

bp = Blueprint('tasks', __name__, url_prefix='/tasks')

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
        patterns_file = parse_patterns(form, all_files)
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
            data_files=data_files
        )
        return redirect(url_for('tasks.show', id=task.task_id))

    return render_template('tasks/edit.html', task=task)

    

@bp.route('/<string:id>', methods=['GET'])
def show(id):
    task = Task.fetch(id)
    annos = fetch_all_annotations(task.task_id)
    return render_template('tasks/show.html',
        task=task,
        annos=annos)

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
        )
        return redirect(url_for('tasks.show', id=task.task_id))






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

def parse_patterns(form, all_files):
    patterns = form.getlist('patterns')
    assert isinstance(patterns, list), "Patterns is not a list"

    if len(patterns) > 0:
        assert len(patterns) == 1, "Only 1 pattern file should be selected"
        patterns_file = patterns[0]
        assert patterns_file in all_files, "Pattern file does not exist"
        return patterns_file
    else:
        # Note a patterns file is optional
        return None
