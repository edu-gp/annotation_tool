from datetime import datetime

import pytest

from alchemy.admin_server.external_services import GCPPubSubService
from alchemy.train.gs_utils import _message_constructor_alchemy_to_gdp


def test_publish_message(monkeypatch):
    class MockResponse:

        @staticmethod
        def result():
            return "published_message_id"

    def mock_publish(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(GCPPubSubService.publish_client,
                        "publish", mock_publish)

    result = GCPPubSubService.publish_message(
        project_id="test_project",
        topic_name="test_topic",
        message_constructor=_message_constructor_alchemy_to_gdp,
        dataset_name='taxonomy',
        prod_inference_url="test_123",
        prod_metadata_url="test_234",
        timestamp=datetime.now().timestamp()
    )

    assert result == "published_message_id"


def test_publish_message_exception(monkeypatch):
    class MockResponse:

        @staticmethod
        def result():
            raise Exception("Message publishing failed.")

    def mock_publish(*args, **kwargs):
        return MockResponse()

    monkeypatch.setattr(GCPPubSubService.publish_client,
                        "publish", mock_publish)

    with pytest.raises(Exception, match="Message publishing failed."):
        GCPPubSubService.publish_message(
            project_id="test_project",
            topic_name="test_topic",
            message_constructor=_message_constructor_alchemy_to_gdp,
            dataset_name='taxonomy',
            prod_inference_url="test_123",
            prod_metadata_url="test_234",
            timestamp=datetime.now().timestamp()
        )
