from alchemy.admin_server import api
from tests.fixtures import admin_server_client  # noqa


def test_hc(admin_server_client):
    response = admin_server_client.get("/api/hc")
    assert response.status == "200 OK"


def make_request(
    admin_server_client,
    auth_token="test123",
    dataset_name="blah.jsonl",
    request_id="R2D2",
):
    url = "/api/trigger_inference"
    data = {}
    if dataset_name:
        data["dataset_name"] = dataset_name
    if request_id:
        data["request_id"] = request_id
    return admin_server_client.post(
        url, headers={"Authorization": f"Bearer {auth_token}"}, json=data
    )


def test_authorized_call_token_not_set(admin_server_client, monkeypatch):
    # Here, API_TOKEN env var is not set.
    monkeypatch.delitem(admin_server_client.application.config, "API_TOKEN", raising=False)
    response = make_request(admin_server_client)
    assert response.status == "500 INTERNAL SERVER ERROR"


def test_unauthorized_call(admin_server_client, monkeypatch):
    monkeypatch.setitem(admin_server_client.application.config, "API_TOKEN", "test123")
    monkeypatch.setattr(api, "run_inference_on_data", lambda *x: None)

    response = make_request(admin_server_client, auth_token="bad-token")
    assert response.status == "401 UNAUTHORIZED"


def test_authorized_call(admin_server_client, monkeypatch):
    monkeypatch.setitem(admin_server_client.application.config, "API_TOKEN", "test123")
    monkeypatch.setattr(api, "run_inference_on_data", lambda *x: None)

    response = make_request(admin_server_client)
    assert response.status == "200 OK"


def test_request_with_no_dataset_name(admin_server_client, monkeypatch):
    monkeypatch.setitem(admin_server_client.application.config, "API_TOKEN", "test123")
    monkeypatch.setattr(api, "run_inference_on_data", lambda *x: None)

    response = make_request(admin_server_client, dataset_name="")
    assert response.status == "400 BAD REQUEST"


def test_dataset_name_thru_query_param(admin_server_client, monkeypatch):
    captured_calls = []

    def capture_call(x, config):
        captured_calls.append(x)

    monkeypatch.setitem(admin_server_client.application.config, "API_TOKEN", "test123")
    monkeypatch.setattr(api, "run_inference_on_data", capture_call)

    # With Query Param
    make_request(admin_server_client, dataset_name="blah_123.jsonl")
    assert captured_calls == ["blah_123.jsonl"]
