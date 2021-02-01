import importlib


class _Auth:
    def __init__(self):
        self._check_login_decorator = None

    def update_login_decorator(self, decorator):
        self._check_login_decorator = decorator

    def login_required(self, f):
        if self._check_login_decorator:
            return self._check_login_decorator(f)
        return f


def init_app(app, auth):
    auth_backend_name = app.config['AUTH_BACKEND']
    auth_backend = importlib.import_module(auth_backend_name)
    auth_backend.init_app(app, auth)


auth = _Auth()
