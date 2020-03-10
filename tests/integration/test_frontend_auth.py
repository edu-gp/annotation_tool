import os
import base64

import pytest

from frontend import create_app
from shared.frontend_user_password import (
    get_frontend_user_password,
    _generate_frontend_user_login_path
)

@pytest.fixture
def client(monkeypatch):
    monkeypatch.setenv('ANNOTATION_TOOL_FRONTEND_SECRET', 'asdsad')

    app = create_app()

    with app.test_client() as client:
        yield client

def _assert_has_logged_in(response, username):
    assert response.status == '200 OK'
    # If the user signed in, there should be a logout link
    assert 'href="/auth/logout"' in response.get_data().decode()
    # If the user signed in, there shouldn't be a word "Login" on the page again
    assert 'Login' not in response.get_data().decode()
    # Also the username itself is displayed on the page
    assert username in response.get_data().decode()

def test_login_with_post(client):
    response = client.post('/auth/login', data=dict(
        username='blah',
        password=get_frontend_user_password('blah')
    ), follow_redirects=True)

    _assert_has_logged_in(response, 'blah')

def test_login_with_link(client):
    path = _generate_frontend_user_login_path('a_very_long_username')
    response = client.get(path, follow_redirects=True)

    _assert_has_logged_in(response, 'a_very_long_username')

def test_login_with_post_wrong_password(client):
    response = client.post('/auth/login', data=dict(
        username='blah',
        password='wrong_password'
    ), follow_redirects=True)
    
    assert response.status == '200 OK'
    assert 'Incorrect password' in response.get_data().decode()
    with pytest.raises(Exception):
        _assert_has_logged_in(response, 'blah')
