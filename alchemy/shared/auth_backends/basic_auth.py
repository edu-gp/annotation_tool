import flask
import flask_login
from envparse import env

from alchemy.db.model import db, User

master_password = env("ANNOTATION_TOOL_ADMIN_SERVER_PASSWORD")


def authenticate(username, password):
    user_model = db.session.query(User).filter_by(username=username).one_or_none()
    if not user_model:
        return None
    if password == master_password:
        return user_model

    return None


def _create_blueprint():
    bp = flask.Blueprint('auth', 'basic_auth')

    # Start log in
    @bp.route('/login', methods=['GET', 'POST'])
    def login():
        error = False
        if flask.request.method == 'POST':
            # Check password
            username = flask.request.form['username']
            password = flask.request.form['password']
            user = authenticate(username, password)
            if user:
                flask_login.login_user(user, remember=True)
                url = flask.url_for('index')
                flask.flash("Welcome to alchemy!", category='info')
                return flask.redirect(url)
            flask.flash("Incorrect username or password.", category='error')
            error = True

        return flask.render_template('auth/login.html', error=error)

    @bp.route("/logout")
    def logout():
        flask_login.logout_user()
        return flask.redirect(flask.url_for('index'))

    return bp


def init_app(app, auth):
    login_manager = flask_login.LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = None

    app.register_blueprint(_create_blueprint(), url_prefix='/auth/')

    auth.update_login_decorator(flask_login.login_required)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.query(User).filter_by(id=user_id).one_or_none()
