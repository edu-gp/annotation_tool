from datetime import datetime

from google.cloud.pubsub_v1 import publisher
from google.auth import credentials
import mockito
import pytest

from alchemy.admin_server.external_services import GCPPubSubService
from alchemy.train.gs_utils import _message_constructor_alchemy_to_gdp


def _patch_client(mockResponse, monkeypatch):
    creds = mockito.mock(spec=credentials.Credentials)
    client = publisher.Client(credentials=creds)

    def mock_publish(*args, **kwargs):
        return mockResponse()

    def get_client():
        return client

    monkeypatch.setattr(client, "publish", mock_publish)
    monkeypatch.setattr(GCPPubSubService, "get_client", get_client)


def test_publish_message(monkeypatch):
    class MockResponse:
        @staticmethod
        def result():
            return "published_message_id"

    _patch_client(MockResponse, monkeypatch)

    result = GCPPubSubService.publish_message(
        project_id="test_project",
        topic_name="test_topic",
        message_constructor=_message_constructor_alchemy_to_gdp,
        dataset_name="taxonomy",
        prod_inference_url="test_123",
        prod_metadata_url="test_234",
        timestamp=datetime.now().timestamp(),
    )

    assert result == "published_message_id"


def test_publish_message_exception(monkeypatch):
    class MockResponse:
        @staticmethod
        def result():
            raise Exception("Message publishing failed.")

    _patch_client(MockResponse, monkeypatch)

    with pytest.raises(Exception, match="Message publishing failed."):
        GCPPubSubService.publish_message(
            project_id="test_project",
            topic_name="test_topic",
            message_constructor=_message_constructor_alchemy_to_gdp,
            dataset_name="taxonomy",
            prod_inference_url="test_123",
            prod_metadata_url="test_234",
            timestamp=datetime.now().timestamp(),
        )
