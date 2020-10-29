import logging
import os

from flask import Flask, redirect, url_for

from alchemy.db.model import db
from .auth import auth


def _setup_logging(config):
    from google.cloud import logging as glog
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

    client = glog.Client()

    handler = CloudLoggingHandler(client, name=config["ADMIN_SERVER_LOGGER"])
    logging.getLogger().setLevel(logging.INFO)
    setup_logging(handler)


def _load_config(config, config_map=None, config_map_replace=False):
    assert not(config_map_replace and config_map is None)
    if config_map_replace:
        config.from_mapping(config_map)
        return

    if not config.from_envvar('ALCHEMY_CONFIG', silent=True):
        logging.warning("ALCHEMY_CONFIG is not set, falling back to config/local.py")
        config.from_pyfile('../alchemy/config/local.py', silent=False)  # Fallback for backwards compatibility

    if config_map:
        config.update(config_map)


def create_app(config_map=None, config_map_replace=False):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    _load_config(app.config, config_map, config_map_replace)
    if app.config['USE_CLOUD_LOGGING']:
        _setup_logging(app.config)

    if app.config['GOOGLE_AI_PLATFORM_ENABLED']:
        from alchemy.train import gs_url
        gs_url.GOOGLE_AI_PLATFORM_BUCKET = app.config['GOOGLE_AI_PLATFORM_BUCKET']

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    db.init_app(app)
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

    @app.route("/ok")
    def hello():
        return "ok"

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
