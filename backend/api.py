from flask import (
    Blueprint, flash, redirect, render_template, request, url_for
)

from db.utils import get_all_data_files

# from .auth import auth

bp = Blueprint('api', __name__, url_prefix='/api')


# TODO to double-check
# [ ] Authentication (token?)
# [ ] JSON format?
# [ ] I need just the filename, if it's _already_ in the gs://alchemy-gp/data folder.
# [ ] Response is 200 if we're able to find the file. It'll be, like, 400, otherwise.
# [ ] Finalize the PubSub topic to get data back into Data Platform.

# TODO: What kind of authentication should we provide for the API?
# @auth.login_required
# def _before_request():
#     """ Auth required for all routes in this module """
#     pass


# bp.before_request(_before_request)


@bp.route('/hc', methods=['GET'])
def index():
    return "OK", 200


@bp.route('/data', methods=['POST'])
def data():
    """
    Inform the system that a new data file is present.

    Currently this is tightly coupled with "what happens after we receive a new
    data file". The logic is simple enough for now, but could use some
    refactoring at a later time.
    """

    # TODO json body?
    # Get the data file
    data_fname = ""

    # TODO
    # Double-check this file exists (rsync it locally?)

    # If it exists...
    # TODO Async...
    run_inference_on_new_data(data_fname)

    # TODO
    # Return True if we're able to find the file, false otherwise.
    return "OK", 200


def run_inference_on_new_data(data_fname):
    # TODO
    # Get all the models are ready to be ran

    # TODO
    # submit_gcp_inference on the models on the newest data
    # TODO if the file already exists remotely, do we still need it locally? (Some ops might depend on it; might need to fetch from cloud as needed)

    # TODO
    # For all existing models, as they're trained, should we also send a message to push them to the data platform? Or should they be picked up next week?
    pass
