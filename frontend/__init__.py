import logging
import os

from flask import (
    Flask, render_template, g
)

from db.model import db
from db.config import DevelopmentConfig
from .auth import login_required

from db.task import Task
from ar.data import fetch_tasks_for_user, fetch_tasks_for_user_from_db


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    # TODO add credentials for sqlite, probably from os.environ
    app.config.from_mapping(
        SECRET_KEY='athena_todo_change_this_in_prod',
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        # app.config.from_pyfile('config.py', silent=True)
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

    @app.route('/ok')
    def hello():
        return 'ok'

    @app.route('/')
    @login_required
    def index():
        username = g.user['username']
        task_ids = fetch_tasks_for_user(username)
        task_id_and_name_pairs = fetch_tasks_for_user_from_db(
            db.session, username)
        # tasks = [Task.fetch(task_id) for task_id in task_ids]
        return render_template('index.html', tasks=task_id_and_name_pairs)

    @app.route('/secret')
    @login_required
    def secret():
        return render_template('secret.html')

    from . import auth
    app.register_blueprint(auth.bp)

    from . import tasks
    app.register_blueprint(tasks.bp)

    from . import labels
    app.register_blueprint(labels.bp)

    return app


'''
env FLASK_APP=frontend FLASK_ENV=development flask init-db

env FLASK_APP=frontend FLASK_ENV=development flask run
'''
