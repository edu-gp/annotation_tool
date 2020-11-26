from envparse import env
from flask_oidc import OpenIDConnect


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
    app.config.update({
        "OIDC_CLIENT_SECRETS": env('OIDC_CLIENT_SECRETS', '/app/.conf/okta/client_secrets.json'),
        "OIDC_COOKIE_SECURE": False,
        "OIDC_CALLBACK_ROUTE": "/auth/oktacallback",
        "OIDC_SCOPES": ["openid", "email", "profile"],
        "OIDC_ID_TOKEN_COOKIE_NAME": "oidc_token",
    })

    oidc = OpenIDConnect(app)

    @app.before_request
    def before_request():
        from flask import g
        if oidc.user_loggedin:
            user_info = oidc.user_getinfo(['sub', 'email', 'name'])
            l = user_info['email']
            if '@' in l:
                user_info['email'] = l[:l.index('@')]

            g.user = user_info
        else:
            g.user = None

    auth.update_login_decorator(oidc.require_login)


auth = _Auth()
