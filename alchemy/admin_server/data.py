from envparse import env
from flask import Blueprint, render_template

from alchemy.db.utils import get_all_data_files
from .auth import auth

bp = Blueprint("data", __name__, url_prefix="/data")


@auth.login_required
def _before_request():
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route("/", methods=["GET"])
def index():
    data_store = env('STORAGE_BACKEND')
    return render_template("data/index.html", data_fnames=get_all_data_files(data_store=data_store))
