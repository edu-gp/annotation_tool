import os
from flask import Blueprint, request, abort

bp = Blueprint('api', __name__, url_prefix='/api')


def get_bearer_token(headers: dict):
    token = None
    if 'Authorization' in headers:
        auth = headers['Authorization']
        if auth.startswith('Bearer '):
            token = auth[len('Bearer '):]
    return token


def _before_request():
    # Let healthcheck bypass auth
    if not request.endpoint.endswith('.healthcheck'):
        # Check token auth
        target_token = os.environ.get('API_TOKEN')
        if target_token is None:
            return abort(500)
        if get_bearer_token(request.headers) != target_token:
            return abort(401)


bp.before_request(_before_request)


@bp.route('/hc', methods=['GET'])
def healthcheck():
    return "OK", 200


@bp.route('/trigger_inference', methods=['POST'])
def trigger_inference():
    """Trigger inference on a dataset."""
    filename = None

    if 'filename' in request.form:
        filename = request.form.get('filename')

    if filename:
        run_inference_on_data(filename)
        return "OK", 200
    else:
        return abort(400)


def run_inference_on_data(data_fname):
    # TODO LEFT OFF HERE. Move all of this into a backend job. Adjust the test monkeypatch accordingly.
    raise Exception("MONKEYPATCH FAILED")

    # TODO is this needed?
    # from db.utils import get_all_data_files

    # TODO
    # Double-check this file exists (rsync it locally?)

    # TODO
    # Get all the models are ready to be ran

    # TODO
    # submit_gcp_inference on the models on the newest data
    # TODO if the file already exists remotely, do we still need it locally? (Some ops might depend on it; might need to fetch from cloud as needed)

    # TODO
    # For all existing models, as they're trained, should we also send a message to push them to the data platform? Or should they be picked up next week?
    pass
