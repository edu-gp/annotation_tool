from tests.fixtures import *
import pytest
from shared.frontend_path_finder import (
    get_frontend_user_password,
    _generate_frontend_user_login_path
)


def _assert_has_logged_in(response, username):
    assert response.status == '200 OK'
    # If the user signed in, there should be a logout link
    assert 'href="/auth/logout"' in response.get_data().decode()
    # If the user signed in, there shouldn't be a word "Login" on the page again
    assert 'Login' not in response.get_data().decode()
    # Also the username itself is displayed on the page
    assert username in response.get_data().decode()


def test_login_with_post(frontend_client):
    response = frontend_client.post('/auth/login', data=dict(
        username='blah',
        password=get_frontend_user_password('blah')
    ), follow_redirects=True)

    _assert_has_logged_in(response, 'blah')


def test_login_with_link(frontend_client):
    path = _generate_frontend_user_login_path('a_very_long_username')
    response = frontend_client.get(path, follow_redirects=True)

    _assert_has_logged_in(response, 'a_very_long_username')


def test_login_with_post_wrong_password(frontend_client):
    response = frontend_client.post('/auth/login', data=dict(
        username='blah',
        password='wrong_password'
    ), follow_redirects=True)

    assert response.status == '200 OK'
    assert 'Incorrect password' in response.get_data().decode()
    with pytest.raises(Exception):
        _assert_has_logged_in(response, 'blah')
