import os

from envparse import env
from flask import Flask, redirect, url_for

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import db
from alchemy.shared import auth_backends, cloud_logging, health_check


def create_app(test_config=None):
    if cloud_logging.is_enabled():
        name = env("ADMIN_SERVER_LOGGER", default="alchemy-admin-server")
        cloud_logging.setup(name)

    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_object(DevelopmentConfig)
    else:
        # load the test config if passed in
        app.config.from_object(test_config)

    app.config.update({
        'SECRET_KEY': env('SECRET_KEY'),
        'AUTH_BACKEND': 'alchemy.shared.auth_backends.saml',
        'SAML_METADATA_URL': env('SAML_METADATA_URL', default=None),
    })

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
    auth_backends.init_app(app, auth_backends.auth)
    auth = auth_backends.auth
    # -------------------------------------------------------------------------
    # Register custom Jinja Filters

    import json

    @app.template_filter("to_pretty_json")
    def to_pretty_json(value):
        try:
            return json.dumps(value, sort_keys=False, indent=4, separators=(",", ": "))
        except Exception as e:
            return str(e)

    # -------------------------------------------------------------------------
    # Routes
    @app.route("/")
    @auth.login_required
    def index():
        return redirect(url_for("tasks.index"))

    @app.route("/status")
    def status_page():
        status = dict()
        status_map = {True: 'ok', False: 'error'}
        status['web'] = status_map[True]
        status['celery'] = status_map[health_check.check_celery()]
        return json.dumps(status)

    # TODO insecure way to access local files
    from flask import request, send_file
    from alchemy.db.fs import filestore_base_dir

    @app.route("/file", methods=["GET"])
    @auth.login_required
    def get_file():
        """
        localhost:5000/tasks/file?f=/tmp/output.png
        """
        path = request.args.get("f")
        if path.startswith(filestore_base_dir()):
            if path[0] != "/":
                # Relative path. Set it to project root.
                path = "../" + path
            return send_file(path)
        else:
            raise Exception(f"Not allowed to send {path}")

    from . import tasks

    app.register_blueprint(tasks.bp)

    from . import models

    app.register_blueprint(models.bp)

    from . import labels

    app.register_blueprint(labels.bp)

    from . import data

    app.register_blueprint(data.bp)

    from . import annotations

    app.register_blueprint(annotations.bp)

    from . import api

    app.register_blueprint(api.bp)

    return app


"""
env FLASK_APP=admin_server FLASK_ENV=development flask init-db

env FLASK_APP=admin_server FLASK_ENV=development flask run
"""
