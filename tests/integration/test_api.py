from tests.fixtures import *
from backend import api


def test_hc(backend_client):
    response = backend_client.get('/api/hc')
    assert response.status == '200 OK'


def make_request(backend_client, auth_token="test123", filename="blah.jsonl"):
    url = '/api/trigger_inference'
    data = {}
    if filename:
        data['filename'] = filename
    return backend_client.post(url, headers={
        'Authorization': f'Bearer {auth_token}'
    }, data=data)


def test_authorized_call_token_not_set(backend_client, monkeypatch):
    # Here, API_TOKEN env var is not set.
    monkeypatch.setattr(api, 'run_inference_on_data', lambda x: None)

    response = make_request(backend_client)
    assert response.status == '500 INTERNAL SERVER ERROR'


def test_unauthorized_call(backend_client, monkeypatch):
    monkeypatch.setenv('API_TOKEN', 'test123')
    monkeypatch.setattr(api, 'run_inference_on_data', lambda x: None)

    response = make_request(backend_client, auth_token='bad-token')
    assert response.status == '401 UNAUTHORIZED'


def test_authorized_call(backend_client, monkeypatch):
    monkeypatch.setenv('API_TOKEN', 'test123')
    monkeypatch.setattr(api, 'run_inference_on_data', lambda x: None)

    response = make_request(backend_client)
    assert response.status == '200 OK'


def test_request_with_no_filename(backend_client, monkeypatch):
    monkeypatch.setenv('API_TOKEN', 'test123')
    monkeypatch.setattr(api, 'run_inference_on_data', lambda x: None)

    response = make_request(backend_client, filename='')
    assert response.status == '400 BAD REQUEST'


def test_filename_thru_query_param(backend_client, monkeypatch):
    captured_calls = []

    def capture_call(x):
        captured_calls.append(x)

    monkeypatch.setenv('API_TOKEN', 'test123')
    monkeypatch.setattr(api, 'run_inference_on_data', capture_call)

    # With Query Param
    make_request(backend_client, filename='blah_123.jsonl')
    assert captured_calls == ['blah_123.jsonl']
