import logging

from ar.data import (
    fetch_all_ar, fetch_ar, fetch_annotation, fetch_all_annotation_ids,
    get_next_ar,
    build_empty_annotation, annotate_ar,
    fetch_labels_by_entity_type, save_labels_by_entity_type)
import json
from flask import (
    Blueprint, g, render_template, request, url_for, jsonify, Response)

from db.task import Task

from .auth import login_required

bp = Blueprint('labels', __name__, url_prefix='/labels')


@bp.route('/fetch_by_entity_type', methods=['GET'])
# @login_required
def fetch_all_labels():
    entity = request.args["entity_type"]
    labels = fetch_labels_by_entity_type(entity)
    return jsonify(labels)


@bp.route('/save', methods=['POST'])
# @login_required
def save_labels():
    data = json.loads(request.data)
    entity_type_name = data['entity_type']
    new_labels = set(data['labels'])
    labels = set(fetch_labels_by_entity_type(entity_type_name))
    logging.error("Existing labels: {}".format(labels))
    new_labels = new_labels.difference(labels)
    try:
        logging.error("Updating new labels {} for EntityType {}".format(
            str(new_labels), entity_type_name
        ))
        save_labels_by_entity_type(entity_type_name, list(new_labels))
        msg = "Labels for entity {} have been updated".format(entity_type_name)
        return Response(msg, status=200, mimetype='application/json')
    except Exception as e:
        logging.error(e)
        return Response(str(e), status=500, mimetype='application/json')
