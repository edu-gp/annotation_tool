from tests.fixtures import *
import base64


def test_password_required(backend_client):
    response = backend_client.get('/', follow_redirects=True)
    assert response.status == '401 UNAUTHORIZED'

    response = backend_client.get('/tasks', follow_redirects=True)
    assert response.status == '401 UNAUTHORIZED'


def test_can_login(backend_client):
    valid_credentials = base64.b64encode(b'testuser:password').decode('utf-8')
    response = backend_client.get(
        '/tasks',
        headers={'Authorization': 'Basic ' + valid_credentials},
        follow_redirects=True)
    assert response.status == '200 OK'


def test_bad_password(backend_client):
    valid_credentials = base64.b64encode(
        b'testuser:incorrect_password').decode('utf-8')
    response = backend_client.get(
        '/tasks',
        headers={'Authorization': 'Basic ' + valid_credentials},
        follow_redirects=True)
    assert response.status == '401 UNAUTHORIZED'
