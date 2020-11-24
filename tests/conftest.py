from .sqlalchemy_conftest import *  # noqa


@pytest.fixture(scope="session", autouse=True)
def set_up_gcs_mock_tempdir(tmp_path_factory):
    from .okta_mock import _Auth
    from alchemy.admin_server import auth
    auth.auth, auth.__auth = _Auth(), auth.auth
    auth.init_app, auth.__init_app = (lambda app: None), auth.init_app

    class ReverseMock:
        def __init__(self):
            self.bypass_original = None

        def __enter__(self):
            self.bypass_original = auth.auth.bypass
            auth.auth.bypass = False

        def __exit__(self, exc_type, exc_val, exc_tb):
            auth.auth.bypass = self.bypass_original

    auth.ReverseMock = ReverseMock
