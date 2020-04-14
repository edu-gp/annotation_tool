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
