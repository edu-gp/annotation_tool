from .sqlalchemy_conftest import *  # noqa


@pytest.fixture(scope="session", autouse=True)
def set_up_gcs_mock_tempdir(tmp_path_factory):
    from .okta_mock import _Auth
    from alchemy.shared import okta
    okta.auth, okta.__auth = _Auth(), okta.auth
    okta.init_app, okta.__init_app = (lambda app, auth: None), okta.init_app

    class ReverseMock:
        def __init__(self):
            self.bypass_original = None

        def __enter__(self):
            self.bypass_original = okta.auth.bypass
            okta.auth.bypass = False

        def __exit__(self, exc_type, exc_val, exc_tb):
            okta.auth.bypass = self.bypass_original

    okta.ReverseMock = ReverseMock
