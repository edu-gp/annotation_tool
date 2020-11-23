from envparse import env
from flask_oidc import OpenIDConnect
from okta import UsersClient


class _Auth:
    def __init__(self):
        self._check_login_decorator = None

    def update_login_decorator(self, decorator):
        self._check_login_decorator = decorator

    def login_required(self, f):
        if self._check_login_decorator:
            return self._check_login_decorator(f)
        return f


auth = _Auth()


def init_app(app):
    app.config["OIDC_CLIENT_SECRETS"] = env('OIDC_CLIENT_SECRETS', '/app/.conf/okta/client_secrets.json')
    app.config["OIDC_COOKIE_SECURE"] = False
    app.config["OIDC_CALLBACK_ROUTE"] = "/auth/oktacallback"
    app.config["OIDC_SCOPES"] = ["openid", "email", "profile"]
    app.config["OIDC_ID_TOKEN_COOKIE_NAME"] = "oidc_token"

    oidc = OpenIDConnect(app)
    okta_client = UsersClient(env("OKTA_ORG_URL"), env("OKTA_AUTH_TOKEN"))

    @app.before_request
    def before_request():
        from flask import g
        if oidc.user_loggedin:
            g.user = okta_client.get_user(oidc.user_getfield("sub"))
        else:
            g.user = None

    auth.update_login_decorator(oidc.require_login)
