import logging

from ar.data import (
    fetch_all_ar, fetch_ar, fetch_annotation, fetch_all_annotation_ids,
    get_next_ar,
    build_empty_annotation, annotate_ar,
    fetch_labels_by_entity, save_labels_by_entity)
import json
from flask import (
    Blueprint, g, render_template, request, url_for, jsonify, Response)

from db.task import Task

from .auth import login_required

bp = Blueprint('labels', __name__, url_prefix='/labels')


@bp.route('/fetch_by_entity', methods=['GET'])
@login_required
def fetch_all_labels():
    entity = request.args["entity"]
    labels = fetch_labels_by_entity(entity)
    return jsonify(labels)


@bp.route('/save', methods=['POST'])
# @login_required
def save_labels():
    data = json.loads(request.data)
    entity = data['entity']
    new_labels = data['labels']
    labels = set(fetch_labels_by_entity(entity))
    labels.update(new_labels)
    try:
        save_labels_by_entity(entity, list(labels))
        msg = "Labels for entity {} have been updated".format(entity)
        return Response(msg, status=200, mimetype='application/json')
    except Exception as e:
        logging.error(e)
        return Response(str(e), status=500, mimetype='application/json')


@bp.route('/<string:id>/annotate/<string:ar_id>')
@login_required
def annotate(id, ar_id):
    user_id = g.user['username']

    # import time
    # st = time.time()

    task = Task.fetch(id)
    ar = fetch_ar(id, user_id, ar_id)
    next_ar_id = get_next_ar(id, user_id, ar_id)

    anno = fetch_annotation(id, user_id, ar_id)

    if anno is None:
        anno = build_empty_annotation(ar)

    anno['suggested_labels'] = task.labels
    anno['task_id'] = task.task_id

    # et = time.time()
    # print("Load time", et-st)

    return render_template('tasks/annotate.html',
                           task=task,
                           anno=anno,
                           # You can pass more than one to render multiple examples
                           # TODO XXX left off here - make this work in the frontend
                           # 0. Create a test kitchen sink page.
                           # 1. Make sure the buttons remember state.
                           data=json.dumps([anno]),
                           next_ar_id=next_ar_id)


@bp.route('/receive_annotation', methods=['POST'])
@login_required
def receive_annotation():
    '''API meant for Javascript to consume'''
    user_id = g.user['username']

    data = json.loads(request.data)

    task_id = data['task_id']
    ar_id = data['req']['ar_id']
    anno = data['anno']

    annotate_ar(task_id, user_id, ar_id, anno)

    next_ar_id = get_next_ar(task_id, user_id, ar_id)

    if next_ar_id:
        return {'redirect': url_for('tasks.annotate', id=task_id, ar_id=next_ar_id)}
    else:
        return {'redirect': url_for('tasks.show', id=task_id)}
