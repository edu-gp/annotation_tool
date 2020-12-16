import functools

from flask import (
    Blueprint,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
    abort,
)

from alchemy.db.model import User, db
from alchemy.shared.annotation_server_path_finder import (
    get_annotation_server_user_password,
)

bp = Blueprint("auth", __name__, url_prefix="/auth")


def _log_out():
    session.clear()
    load_logged_in_user()


@bp.route("/login", methods=("GET", "POST"))
def login():
    if request.method == "GET":
        # This came from a sign-in link. Try to sign in this user immediately.
        username = request.args.get("username")
        password = request.args.get("password")
    elif request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
    else:
        abort(405)
        return  # noqa

    if username and password:
        user = db.session.query(User).filter_by(username=username).one_or_none()
        expected_password = get_annotation_server_user_password(username)
        # This is to prevent short circuiting the conditions and protect against timing
        # side channel attacks.
        conditions = [
            expected_password == password,
            user is not None,
        ]
        if all(conditions):
            session.clear()
            session["user_id"] = username
            session["username"] = username
            return redirect(url_for("index"))
        else:
            _log_out()
            flash("Incorrect username or password, please use a fresh log in link.")

    return render_template("auth/login.html")


@bp.route("/logout")
def logout():
    _log_out()
    return redirect(url_for("index"))


@bp.before_app_request
def load_logged_in_user():
    # user_id = session.get('user_id')

    # if user_id is None:
    #     g.user = None
    # else:
    #     g.user = get_db().execute(
    #         'SELECT * FROM user WHERE id = ?', (user_id,)
    #     ).fetchone()

    if session.get("user_id") is None:
        g.user = None
    else:
        user = db.session.query(User).filter_by(username=session.get("user_id")).one_or_none()
        if not user:
            flash("Login expired")
            _log_out()

        # TODO: replace this whole dictionary with the user object (rel. okta)
        g.user = {
            "user_id": session.get("user_id"),
            "username": str(session.get("username")),
        }


def login_required(view):
    @functools.wraps(view)
    def wrapped_view(**kwargs):
        if g.user is None:
            return redirect(url_for("auth.login"))

        return view(**kwargs)

    return wrapped_view
