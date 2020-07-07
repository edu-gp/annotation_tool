from tests.fixtures import *


def test_hc(backend_client):
    response = backend_client.get('/api/hc')
    assert response.status == '200 OK'


def test_authorized_call_token_not_set(backend_client):
    # Here, API_TOKEN env var is not set.
    response = backend_client.post('/api/trigger_inference')
    assert response.status == '500 INTERNAL SERVER ERROR'


def test_unauthorized_call(backend_client, monkeypatch):
    monkeypatch.setenv('API_TOKEN', 'test123')
    response = backend_client.post('/api/trigger_inference')
    assert response.status == '401 UNAUTHORIZED'


def test_authorized_call(backend_client, monkeypatch):
    monkeypatch.setenv('API_TOKEN', 'test123')
    response = backend_client.post('/api/trigger_inference', headers={
        'Authorization': 'Bearer test123'
    })
    assert response.status == '200 OK'
