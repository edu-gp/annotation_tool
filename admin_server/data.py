from flask import (
    Blueprint, flash, redirect, render_template, request, url_for
)

from db.utils import get_all_data_files

from .auth import auth

bp = Blueprint('data', __name__, url_prefix='/data')


@auth.login_required
def _before_request():
    """ Auth required for all routes in this module """
    pass


bp.before_request(_before_request)


@bp.route('/', methods=['GET'])
def index():
    return render_template('data/index.html', data_fnames=get_all_data_files())
