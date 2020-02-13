import os
import uuid
import time
import json

from typing import List

from flask import (
    Blueprint, flash, g, redirect, render_template, request, session, url_for
)

from db.task import Task

from .auth import login_required

bp = Blueprint('tasks', __name__, url_prefix='/tasks')

# TODO eddie update with new way of showing annotations
# TODO this has to be rewritten with
from ar.data import fetch_all_ar, fetch_ar, fetch_all_annotations, get_next_ar

# def load_annotation_requests(task_id, user_id): 
#     task = Task.fetch(task_id)
#     assert task is not None
#     fname = os.path.join(_task_dir(task_id), 'annotators', f'{user_id}.jsonl')
#     data = load_jsonl(fname, to_df=False)
#     return task, data

# from joblib import Memory
# location = './cachedir'
# memory = Memory(location, verbose=0)
# load_annotation_requests_cached = memory.cache(load_annotation_requests)

@bp.route('/<string:id>')
@login_required
def show(id):
    user_id = 'eddie'

    import time
    st = time.time()
    
    task = Task.fetch(id)
    ars = fetch_all_ar(id, user_id)
    annotated = set(fetch_all_annotations(id, user_id))
    has_annotation = [x in annotated for x in ars]

    et = time.time()
    print("Load time", et-st)

    return render_template('tasks/show.html',
        task=task,
        ars=ars,
        has_annotation=has_annotation)

@bp.route('/<string:id>/annotate/<string:ar_id>')
@login_required
def annotate(id, ar_id):
    user_id = 'eddie'

    import time
    st = time.time()

    task = Task.fetch(id)
    ar = fetch_ar(id, user_id, ar_id)
    next_ar_id = get_next_ar(id, user_id, ar_id)

    et = time.time()
    print("Load time", et-st)

    return render_template('tasks/annotate.html',
        task=task,
        # You can pass more than one to render multiple examples
        data=json.dumps([ar]),
        next_ar_id=next_ar_id)
