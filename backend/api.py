import os
from flask import Blueprint, request, abort
from train.train_celery import submit_gcp_inference_on_new_file

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


def run_inference_on_data(filename):
    # TODO: This function exists so I can mock it in test.
    #       Can get rid of it once we set up Celery fixtures.
    submit_gcp_inference_on_new_file.delay(filename)
