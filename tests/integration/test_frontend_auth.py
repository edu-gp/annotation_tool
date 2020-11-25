import pytest


def _assert_has_logged_in(response, username):
    assert response.status == "200 OK"
    # If the user signed in, there should be a logout link
    assert 'href="/auth/logout"' in response.get_data().decode()
    # If the user signed in, there shouldn't be a word "Login" on the page again
    assert "Login" not in response.get_data().decode()
    # Also the username itself is displayed on the page
    assert username in response.get_data().decode()


def test_login_with_post_wrong_password(annotation_server_client):
    response = annotation_server_client.post(
        "/auth/login",
        data=dict(username="blah", password="wrong_password"),
        follow_redirects=True,
    )

    assert response.status == "200 OK"
    assert "Incorrect password" in response.get_data().decode()
    with pytest.raises(Exception):
        _assert_has_logged_in(response, "blah")
