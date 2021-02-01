from flask import Blueprint, render_template

from alchemy.db.utils import get_all_data_files
from alchemy.shared.auth_backends import auth

bp = Blueprint("data", __name__, url_prefix="/data")


@auth.login_required
def _before_request():
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route("/", methods=["GET"])
def index():
    return render_template("data/index.html", data_fnames=get_all_data_files())
