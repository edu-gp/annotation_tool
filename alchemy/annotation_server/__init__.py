import logging
import os

from flask import (
    Flask, render_template, g
)

from alchemy.ar.data import fetch_tasks_for_user_from_db
from alchemy.db.model import db
from .auth import login_required


def _setup_logging(config):
    from google.cloud import logging as glog
    from google.cloud.logging.handlers import CloudLoggingHandler, setup_logging

    client = glog.Client()

    handler = CloudLoggingHandler(client, name=config['ANNOTATION_SERVER_LOGGER'])
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

    @app.route("/ok")
    def hello():
        return "ok"

    @app.route("/")
    @login_required
    def index():
        username = g.user["username"]
        task_id_and_name_pairs = fetch_tasks_for_user_from_db(db.session, username)
        return render_template("index.html", tasks=task_id_and_name_pairs)

    @app.route("/secret")
    @login_required
    def secret():
        return render_template("secret.html")

    from . import auth

    app.register_blueprint(auth.bp)

    from . import tasks

    app.register_blueprint(tasks.bp)

    from . import labels

    app.register_blueprint(labels.bp)

    return app


"""
env FLASK_APP=annotation_server FLASK_ENV=development flask init-db

env FLASK_APP=annotation_server FLASK_ENV=development flask run
"""
