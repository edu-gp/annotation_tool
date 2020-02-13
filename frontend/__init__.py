import os

from flask import (
    Flask, render_template, g, redirect, url_for, session
)

from .auth import login_required

from ar.data import fetch_tasks_for_user

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='athena_todo_change_this_in_prod',
        DATABASE=os.path.join(app.instance_path, 'athena.sqlite'),
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    @app.route('/ok')
    def hello():
        return 'ok'

    @app.route('/')
    @login_required
    def index():
        user_id = session.get('user_id')
        # ... find the user
        username = 'eddie'

        task_ids = fetch_tasks_for_user(username)
        return render_template('index.html', task_ids=task_ids)

    @app.route('/secret')
    @login_required
    def secret():
        return render_template('secret.html')

    from . import auth
    app.register_blueprint(auth.bp)

    from . import tasks
    app.register_blueprint(tasks.bp)

    return app

'''
env FLASK_APP=frontend FLASK_ENV=development flask init-db

env FLASK_APP=frontend FLASK_ENV=development flask run
'''