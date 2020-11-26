from envparse import env
from flask_oidc import OpenIDConnect

from alchemy.cloud_function.inference.inference_api_invoker import create_gcp_client, get_secret


class _Auth:
    def __init__(self):
        self._check_login_decorator = None

    def update_login_decorator(self, decorator):
        self._check_login_decorator = decorator

    def login_required(self, f):
        if self._check_login_decorator:
            return self._check_login_decorator(f)
        return f


class AlchemyOpenIDConnect(OpenIDConnect):
    def load_secrets(self, app):
        organization_name = env("OKTA_ORG_NAME", default='georgian-io')
        okta_base_url = f'https://{organization_name}.okta.com/oauth2/default'
        secret_manager_client = create_gcp_client()
        if app.config['DEBUG']:
            suffix = '-debug'
        else:
            suffix = ''

        okta_client_id = get_secret(
            client=secret_manager_client, project_id=env('GCP_PROJECT_ID'),
            secret_id=f'alchemy-okta-client-id{suffix}')
        okta_client_secret = get_secret(
            client=secret_manager_client, project_id=env('GCP_PROJECT_ID'),
            secret_id=f'alchemy-okta-client-secret{suffix}')

        return {'web': {
            "client_id": okta_client_id,
            "client_secret": okta_client_secret,
            "auth_uri": f"{okta_base_url}/v1/authorize",
            "token_uri": f"{okta_base_url}/v1/token",
            "issuer": f"{okta_base_url}",
            "userinfo_uri": f"{okta_base_url}/userinfo",
        }}


def init_app(app, auth):
    app.config.update({
        "OIDC_CLIENT_SECRETS": None,
        "OIDC_COOKIE_SECURE": False,
        "OIDC_CALLBACK_ROUTE": "/auth/oktacallback",
        "OIDC_SCOPES": ["openid", "email", "profile"],
        "OIDC_ID_TOKEN_COOKIE_NAME": "oidc_token",
    })

    oidc = AlchemyOpenIDConnect(app)

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
