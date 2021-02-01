from .sqlalchemy_conftest import *  # noqa


@pytest.fixture(scope="session", autouse=True)
def set_up_gcs_mock_tempdir(tmp_path_factory):
    from .okta_mock import _Auth
    from alchemy.shared import auth_backends
    auth_backends.auth, auth_backends.__auth = _Auth(), auth_backends.auth
    auth_backends.init_app, auth_backends.__init_app = (lambda app, auth: None), auth_backends.init_app

    class ReverseMock:
        def __init__(self):
            self.bypass_original = None

        def __enter__(self):
            self.bypass_original = auth_backends.auth.bypass
            auth_backends.auth.bypass = False

        def __exit__(self, exc_type, exc_val, exc_tb):
            auth_backends.auth.bypass = self.bypass_original

    auth_backends.ReverseMock = ReverseMock


@pytest.fixture(scope="session", autouse=True)
def disable_cloud_logging():
    import os
    old_val = os.environ.get('USE_CLOUD_LOGGING', default=None)
    os.environ['USE_CLOUD_LOGGING'] = '0'

    yield

    if old_val is None:
        del os.environ['USE_CLOUD_LOGGING']
    else:
        os.environ['USE_CLOUD_LOGGING'] = old_val
