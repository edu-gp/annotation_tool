import logging

import flask
import flask_login
import requests
from saml2 import (
    BINDING_HTTP_POST,
    BINDING_HTTP_REDIRECT,
    entity,
)
from saml2.client import Saml2Client
from saml2.config import Config as Saml2Config
from saml2.saml import NAME_FORMAT_BASIC
from saml2.validate import ResponseLifetimeExceed

from alchemy.db.model import db, User
from alchemy.shared.config import Config


def get_saml_client(*, metadata):
    acs_url = flask.url_for(
        "SAML.idp_initiated",
        _external=True)
    https_acs_url = flask.url_for(
        "SAML.idp_initiated",
        _external=True,
        _scheme='https')

    settings = {
        'metadata': {
            'inline': [metadata],
        },
        'entityid': acs_url,
        'service': {
            'sp': {
                'endpoints': {
                    'assertion_consumer_service': [
                        (acs_url, BINDING_HTTP_REDIRECT),
                        (acs_url, BINDING_HTTP_POST),
                        (https_acs_url, BINDING_HTTP_REDIRECT),
                        (https_acs_url, BINDING_HTTP_POST)
                    ],
                },
                # Don't verify that the incoming requests originate from us via
                # the built-in cache for authn request ids in pysaml2
                'allow_unsolicited': False,
                # Don't sign authn requests, since signed requests only make
                # sense in a situation where you control both the SP and IdP
                'authn_requests_signed': False,
                'logout_requests_signed': True,
                'want_assertions_signed': True,
                'want_response_signed': False,
            },
        },
        "entity_attributes": [
            {
                "name": "urn:mace:dir:attribute-def:givenname",
                "friendly_name": "FirstName",
                "name_format": NAME_FORMAT_BASIC,
                "values": ["any"],
            },
            {
                "name": "urn:mace:dir:attribute-def:surname",
                "friendly_name": "LastName",
                "name_format": NAME_FORMAT_BASIC,
            },
            {
                "name": "urn:mace:dir:attribute-def:emailaddress",
                "friendly_name": "Email",
                "name_format": NAME_FORMAT_BASIC,
            },
        ]
    }
    spConfig = Saml2Config()
    spConfig.load(settings)
    saml_client = Saml2Client(config=spConfig)
    return saml_client


def _create_blueprint(*, metadata):
    bp = flask.Blueprint('SAML', 'saml')

    # SAML Callback:
    @bp.route('/saml/login', methods=['POST'])
    def idp_initiated():
        saml_client = get_saml_client(metadata=metadata)
        try:
            authn_response = saml_client.parse_authn_request_response(
                flask.request.form['SAMLResponse'],
                entity.BINDING_HTTP_POST,
                outstanding={
                    flask.session['saml_reqid']: '/',
                }
            )
        except (ResponseLifetimeExceed, KeyError):
            flask.flash('Login link expired, please log in again.')
            return flask.redirect('/')
        else:
            del flask.session['saml_reqid']

        authn_response.get_identity()
        saml_user_info = authn_response.get_subject()
        username = saml_user_info.text.lower()
        user_info = {
            'username': username,
            'first_name': authn_response.ava.get('givenName', [''])[0],
            'last_name': authn_response.ava.get('surname', [''])[0],
            'email': authn_response.ava.get('emailAddress', [''])[0].lower(),
        }
        user_changed = False
        user_model = db.session.query(User).filter_by(username=username).one_or_none()
        if not user_model:
            user_model = User(
                username=username,
                first_name=user_info['first_name'],
                last_name=user_info['last_name'],
                email=user_info['email'],
            )
            db.session.add(user_model)
            user_changed = True
            logging.info(f"Registered new user {username}")
        else:
            for k, v in user_info.items():
                if getattr(user_model, k) != v:
                    user_changed = True
                    setattr(user_model, k, v)
            if user_changed:
                db.session.add(user_model)
                logging.info(f"Updated user info {username}")

        if user_changed:
            db.session.commit()

        # Relate to user model
        flask_login.login_user(user_model)
        url = flask.url_for('index')
        if 'RelayState' in flask.request.form:
            url = url or flask.request.form['RelayState']
            # TODO: check if url is valid (in the same website)

        logging.debug("redirect to " + str(url))
        return flask.redirect(url)

    # Start log in
    @bp.route('/saml/login')
    def sp_initiated():
        # Handle all logins through the annotator website, to
        # fix the admin callback's malformed request issue.
        redirect_to = _check_admin_server()
        if redirect_to:
            return flask.redirect(redirect_to)

        saml_client = get_saml_client(metadata=metadata)
        reqid, info = saml_client.prepare_for_authenticate()
        flask.session['saml_reqid'] = reqid

        redirect_url = None
        # Select the IdP URL to send the AuthN request to
        for key, value in info['headers']:
            if key is 'Location':
                redirect_url = value
        response = flask.redirect(redirect_url, code=302)
        # NOTE:
        #   I realize I _technically_ don't need to set Cache-Control or Pragma:
        #     http://stackoverflow.com/a/5494469
        #   However, Section 3.2.3.2 of the SAML spec suggests they are set:
        #     http://docs.oasis-open.org/security/saml/v2.0/saml-bindings-2.0-os.pdf
        #   We set those headers here as a "belt and suspenders" approach,
        #   since enterprise environments don't always conform to RFCs
        response.headers['Cache-Control'] = 'no-cache, no-store'
        response.headers['Pragma'] = 'no-cache'
        return response

    @bp.route("/logout")
    def logout():
        flask_login.logout_user()
        return flask.redirect(flask.url_for('index'))

    @bp.route("/login")
    @flask_login.login_required
    def login():
        return flask.redirect(flask.url_for('index'))

    def _check_admin_server():
        full_url = flask.request.base_url
        annotation_server_url = Config.get_annotation_server()
        if annotation_server_url not in full_url:
            # I wish I could check the url against SAML SP
            # entity ID, however Okta does not provide it in
            # the metadata. So this if condition is a hacky
            # way to know if we're on admin site.
            return None

        if annotation_server_url[:-1] != '/':
            annotation_server_url = f'{annotation_server_url}/'
        frontend_login_page = full_url.replace(flask.request.url_root, annotation_server_url)

        if not flask_login.current_user.is_authenticated:
            redirect_url = frontend_login_page
        else:
            redirect_url = flask.url_for('index')

        return redirect_url

    return bp


def init_app(app, auth):
    login_manager = flask_login.LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'SAML.sp_initiated'
    login_manager.login_message = None
    metadata_url = app.config.get('SAML_METADATA_URL')
    if metadata_url:
        rv = requests.get(metadata_url)
        metadata = rv.text
    else:
        metadata = ''

    app.register_blueprint(_create_blueprint(metadata=metadata), url_prefix='/auth/')

    auth.update_login_decorator(flask_login.login_required)

    @login_manager.user_loader
    def load_user(user_id):
        return db.session.query(User).filter_by(id=user_id).one_or_none()
