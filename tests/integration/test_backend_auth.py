import base64

from tests.fixtures import *  # noqa


def test_password_required(admin_server_client):
    response = admin_server_client.get("/", follow_redirects=True)
    assert response.status == "401 UNAUTHORIZED"

    response = admin_server_client.get("/tasks", follow_redirects=True)
    assert response.status == "401 UNAUTHORIZED"


def test_can_login(admin_server_client):
    valid_credentials = base64.b64encode(b"testuser:secret").decode("utf-8")
    response = admin_server_client.get(
        "/tasks",
        headers={"Authorization": "Basic " + valid_credentials},
        follow_redirects=True,
    )
    assert response.status == "200 OK"


def test_bad_password(admin_server_client):
    valid_credentials = base64.b64encode(b"testuser:incorrect_password").decode("utf-8")
    response = admin_server_client.get(
        "/tasks",
        headers={"Authorization": "Basic " + valid_credentials},
        follow_redirects=True,
    )
    assert response.status == "401 UNAUTHORIZED"
