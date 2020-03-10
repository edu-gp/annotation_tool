import os

from flask import (
    Flask, render_template, g, redirect, url_for, session
)

from .auth import auth

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
    from db import _task_dir
    @app.route('/file', methods=['GET'])
    @auth.login_required
    def get_file():
        '''
        localhost:5000/tasks/file?f=/tmp/output.png
        '''
        path = request.args.get('f')
        path = os.path.join(_task_dir(), path)
        print("Send file:", path)
        return send_file(path)

    from . import tasks
    app.register_blueprint(tasks.bp)

    return app

'''
env FLASK_APP=backend FLASK_ENV=development flask init-db

env FLASK_APP=backend FLASK_ENV=development flask run
'''