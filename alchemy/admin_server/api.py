import logging

from flask import Blueprint, request, abort, current_app

from alchemy.train.train_celery import submit_gcp_inference_on_new_file

bp = Blueprint("api", __name__, url_prefix="/api")


def get_bearer_token(headers: dict):
    token = None
    if "Authorization" in headers:
        auth = headers["Authorization"]
        if auth.startswith("Bearer "):
            token = auth[len("Bearer ") :]
    return token


def _before_request():
    # Let healthcheck bypass auth
    if not request.endpoint.endswith(".healthcheck"):
        # Check token auth
        try:
            target_token = current_app.config['API_TOKEN']
        except Exception as e:
            logging.error(e)
            target_token = None
        if target_token is None:
            return abort(500)
        if get_bearer_token(request.headers) != target_token:
            return abort(401)


bp.before_request(_before_request)


@bp.route("/hc", methods=["GET"])
def healthcheck():
    return "OK", 200


@bp.route("/trigger_inference", methods=["POST"])
def trigger_inference():
    """Trigger inference on a dataset."""
    dataset_name = None

    json_data = request.get_json()

    if json_data:
        request_id = json_data.get("request_id")
        logging.info("Handling request " + request_id)
        dataset_name = json_data.get("dataset_name", None)

    if dataset_name:
        run_inference_on_data(dataset_name)
        return "OK", 200
    else:
        logging.error(f"[{request_id}] Missing dataset name. Abort...")
        return abort(400)


def run_inference_on_data(dataset_name):
    # TODO: This function exists so I can mock it in test.
    #       Can get rid of it once we set up Celery fixtures.
    submit_gcp_inference_on_new_file.delay(dataset_name)
