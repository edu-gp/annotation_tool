import os
import uuid
import time
import json

from typing import List

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from db.task import Task, DEFAULT_TASK_STORAGE
from shared.utils import load_jsonl

from .auth import login_required

bp = Blueprint('tasks', __name__, url_prefix='/tasks')

def load_annotation_requests(task_id, user_id): 
    task = Task.fetch(task_id)
    assert task is not None
    fname = os.path.join(DEFAULT_TASK_STORAGE, task_id, 'annotators', f'{user_id}.jsonl')
    data = load_jsonl(fname, to_df=False)
    return task, data

from joblib import Memory
location = './cachedir'
memory = Memory(location, verbose=0)
load_annotation_requests_cached = memory.cache(load_annotation_requests)

@bp.route('/<string:id>')
@login_required
def show(id):
    import time
    st = time.time()
    task, data = load_annotation_requests_cached(id, 'eddie')
    et = time.time()
    print(et-st)

    has_annotation = [False for _ in data]

    return render_template('tasks/show.html',
        task=task,
        data=data,
        has_annotation=has_annotation)

@bp.route('/<string:id>/annotate/<int:line>')
@login_required
def annotate(id, line):
    task, data = load_annotation_requests(id, 'eddie')

    return render_template('tasks/annotate.html',
        task=task,
        # You can pass more than one to render multiple examples
        data=json.dumps([data[line]]))
