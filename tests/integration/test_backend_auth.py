import base64
import pytest
from backend import create_app


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_BACKEND_PASSWORD', 'password')

    app = create_app()

    with app.test_client() as client:
        yield client


def test_password_required(client):
    response = client.get('/', follow_redirects=True)
    assert response.status == '401 UNAUTHORIZED'

    response = client.get('/tasks', follow_redirects=True)
    assert response.status == '401 UNAUTHORIZED'


def test_can_login(client):
    valid_credentials = base64.b64encode(b'testuser:password').decode('utf-8')
    response = client.get(
        '/tasks',
        headers={'Authorization': 'Basic ' + valid_credentials},
        follow_redirects=True)
    assert response.status == '200 OK'


def test_bad_password(client):
    valid_credentials = base64.b64encode(
        b'testuser:incorrect_password').decode('utf-8')
    response = client.get(
        '/tasks',
        headers={'Authorization': 'Basic ' + valid_credentials},
        follow_redirects=True)
    assert response.status == '401 UNAUTHORIZED'
