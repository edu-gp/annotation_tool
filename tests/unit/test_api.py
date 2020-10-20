from alchemy.admin_server.api import get_bearer_token


def test_get_bearer_token():
    assert get_bearer_token({"Authorization": "Bearer hello123"}) == "hello123"

    assert get_bearer_token({"Authorization": "Basic NsMjAyMA=="}) is None

    assert get_bearer_token({}) is None
