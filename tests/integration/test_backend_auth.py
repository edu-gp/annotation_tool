from envparse import env

from tests.fixtures import *  # noqa


def test_password_required(admin_server_client):
    response = admin_server_client.get("/", follow_redirects=False)
    print(response, response.headers)
    assert response.status == "302 FOUND"

    response = admin_server_client.get("/tasks/", follow_redirects=False)
    assert response.status == "302 FOUND"
    assert response.headers['Location'].startswith(env('OKTA_ORG_URL') + 'oauth2/default/v1/authorize')
