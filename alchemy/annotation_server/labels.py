import logging

from alchemy.db.model import (
    db, fetch_labels_by_entity_type, save_labels_by_entity_type)
import json
from flask import (
    Blueprint, request, jsonify, Response)

bp = Blueprint('labels', __name__, url_prefix='/labels')


@bp.route('/fetch_by_entity_type', methods=['GET'])
# @login_required
def fetch_all_labels():
    entity_type = request.args["entity_type"]
    labels = fetch_labels_by_entity_type(db.session, entity_type)
    return jsonify(labels)


@bp.route('/save', methods=['POST'])
# @login_required
def save_labels():
    data = json.loads(request.data)
    entity_type_name = data['entity_type']
    new_labels = set(data['labels'])
    labels = set(fetch_labels_by_entity_type(db.session, entity_type_name))
    new_labels = new_labels.difference(labels)
    try:
        logging.info("Updating new labels {} for EntityType {}".format(
            str(new_labels), entity_type_name
        ))
        save_labels_by_entity_type(
            db.session, entity_type_name, list(new_labels))
        msg = "Labels for entity {} have been updated".format(entity_type_name)
        return Response(msg, status=200, mimetype='application/json')
    except Exception as e:
        logging.error(e)
        return Response(str(e), status=500, mimetype='application/json')
