import os

from flask import (
    Flask, redirect, url_for
)

from db.config import DevelopmentConfig
from db.model import db
from .auth import auth


def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='athena_todo_change_this_in_prod',
    )

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
    # -------------------------------------------------------------------------
    # Register custom Jinja Filters

    import json
    @app.template_filter('to_pretty_json')
    def to_pretty_json(value):
        try:
            return json.dumps(value, sort_keys=False,
                              indent=4, separators=(',', ': '))
        except Exception as e:
            return str(e)

    # -------------------------------------------------------------------------
    # Routes

    @app.route('/ok')
    def hello():
        return 'ok'

    @app.route('/')
    @auth.login_required
    def index():
        return redirect(url_for('tasks.index'))

    # TODO insecure way to access local files
    from flask import request, send_file
    from db.fs import filestore_base_dir
    @app.route('/file', methods=['GET'])
    @auth.login_required
    def get_file():
        '''
        localhost:5000/tasks/file?f=/tmp/output.png
        '''
        path = request.args.get('f')
        if path.startswith(filestore_base_dir()):
            if path[0] != '/':
                # Relative path. Set it to project root.
                path = '../' + path
            return send_file(path)
        else:
            raise Exception(f"Not allowed to send {path}")

    from . import tasks
    app.register_blueprint(tasks.bp)

    # --- Admin Routes ---

    import subprocess
    from flask import make_response

    @app.route('/admin/ar_logs')
    @auth.login_required
    def admin_view_ar_logs():
        completed_process = subprocess.run(
            ["supervisorctl", "tail", "-3200", "ar_celery", "stderr"], capture_output=True)
        output = completed_process.stdout.decode()
        response = make_response(output, 200)
        response.content_type = "text/plain"
        return response

    @app.route('/admin/train_logs')
    @auth.login_required
    def admin_view_train_logs():
        completed_process = subprocess.run(
            ["supervisorctl", "tail", "-3200", "train_celery", "stderr"], capture_output=True)
        output = completed_process.stdout.decode()
        response = make_response(output, 200)
        response.content_type = "text/plain"
        return response

    return app


'''
env FLASK_APP=backend FLASK_ENV=development flask init-db

env FLASK_APP=backend FLASK_ENV=development flask run
'''
