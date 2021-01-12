import json
import os

import flask_login
from envparse import env
from flask import (
    Flask, render_template
)

from alchemy.ar.data import fetch_tasks_for_user_from_db
from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import db
from alchemy.shared import okta, cloud_logging, health_check


def create_app(test_config=None):
    if cloud_logging.is_enabled():
        name = env("ANNOTATION_SERVER_LOGGER", default="alchemy-annotation-server")
        cloud_logging.setup(name)

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # TODO add credentials for sqlite, probably from environment vars

    if test_config is None:
        # load the instance config, if it exists, when not testing
        # app.config.from_pyfile('config.py', silent=True)
        app.config.from_object(DevelopmentConfig)
    else:
        # load the test config if passed in
        app.config.from_object(test_config)

    app.config.update({
        'SECRET_KEY': env('SECRET_KEY'),
        'OKTA_BACKEND': 'alchemy.shared.okta.saml',
        'SAML_METADATA_URL': env('SAML_METADATA_URL', default=None),
    })

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
    okta.init_app(app, okta.auth)
    login_required = okta.auth.login_required

    @app.route("/")
    @login_required
    def index():
        username = flask_login.current_user.username
        task_id_and_name_pairs = fetch_tasks_for_user_from_db(db.session, username)
        return render_template("index.html", tasks=task_id_and_name_pairs)

    @app.route("/status")
    def status_page():
        status = dict()
        status_map = {True: 'ok', False: 'error'}
        status['web'] = status_map[True]
        status['filesystem'] = status_map[health_check.check_file_system()]
        return json.dumps(status)

    @app.route("/secret")
    @login_required
    def secret():
        return render_template("secret.html")

    from . import tasks

    app.register_blueprint(tasks.bp)

    from . import labels

    app.register_blueprint(labels.bp)

    return app


"""
env FLASK_APP=annotation_server FLASK_ENV=development flask init-db

env FLASK_APP=annotation_server FLASK_ENV=development flask run
"""
