import logging
import os

from envparse import env
from flask import Flask, redirect, url_for

from alchemy.db.config import DevelopmentConfig
from alchemy.db.model import db
from alchemy.shared import okta

if env.bool("USE_CLOUD_LOGGING", default=False):
    from google.cloud import logging as glog
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

    client = glog.Client()

    handler = CloudLoggingHandler(
        client, name=env("ADMIN_SERVER_LOGGER", default="alchemy-admin-server")
    )
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(SECRET_KEY="athena_todo_change_this_in_prod")

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_object(DevelopmentConfig)
    else:
        # load the test config if passed in
        app.config.from_object(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
    okta.init_app(app, okta.auth)
    auth = okta.auth
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

    @app.route("/auth/login")
    @auth.login_required
    def login():
        return redirect(url_for('index'))

    @app.route("/")
    @auth.login_required
    def index():
        return redirect(url_for("tasks.index"))

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
