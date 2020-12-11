import logging
import os

from envparse import env
from flask import (
    Flask, render_template, g
)

from alchemy.ar.data import fetch_tasks_for_user_from_db
from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import db
from alchemy.shared import okta

if env.bool("USE_CLOUD_LOGGING", default=False):
    from google.cloud import logging as glog
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

    client = glog.Client()

    handler = CloudLoggingHandler(client,
                                  name=env("ANNOTATION_SERVER_LOGGER",
                                           default="alchemy-annotation-server"))
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)


def create_app(test_config=None):
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

    @app.route("/ok")
    def hello():
        return "ok"

    @app.route("/")
    @login_required
    def index():
        username = g.user['email']
        task_id_and_name_pairs = fetch_tasks_for_user_from_db(db.session, username)
        return render_template("index.html", tasks=task_id_and_name_pairs)

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
