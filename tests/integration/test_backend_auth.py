from tests.fixtures import *  # noqa


def test_password_required(admin_server_client):
    from alchemy.shared import auth_backends

    with auth_backends.ReverseMock():
        response = admin_server_client.get("/", follow_redirects=False)
        assert response.status == "302 FOUND"

        response = admin_server_client.get("/tasks/", follow_redirects=False)
        assert response.status == "302 FOUND"
        assert 'oauth2/default/v1/authorize' in response.headers.get('Location', '')
