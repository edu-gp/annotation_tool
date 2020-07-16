import json
from datetime import datetime

import pytest

from backend.external_services import GCPPubSubService


def test__construct_message():
    prod_inference_url = "abc"
    prod_meta_url = "bcd"
    timestamp = datetime.now().timestamp()

    message = GCPPubSubService._construct_message(
        prod_inference_url,
        prod_meta_url,
        timestamp
    )

    expected = {
        'timestamp': timestamp,
        'environment': 'prod',
        # ideally we should have multiple stages and not harding this here.
        'dataset': 'taxonomy_b2c',
        'path': {
            'inferences': prod_inference_url,
            'metadata': prod_meta_url
        }
    }

    assert message == json.dumps(expected)


def test__prepare_pubsub_topic_name(monkeypatch):
    monkeypatch.setenv('GCP_PROJECT_ID', 'test_project')
    monkeypatch.setenv('INFERENCE_OUTPUT_PUBSUB_TOPIC', 'test_topic')
    topic = GCPPubSubService._prepare_pubsub_topic_name()
    expected = 'projects/test_project/topics/test_topic'

    assert topic == expected


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
        prod_inference_url="test_123",
        prod_meta_url="test_234",
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
            prod_inference_url="test_123",
            prod_meta_url="test_234",
            timestamp=datetime.now().timestamp()
        )
