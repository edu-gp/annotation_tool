import json
import logging
import os

from google.cloud import pubsub


class GCPPubSubService:
    publish_client = pubsub.PublisherClient()

    @classmethod
    def publish_message(cls, prod_inference_url, prod_meta_url, timestamp):
        message = cls.__construct_message(prod_inference_url, prod_meta_url,
                                          timestamp)
        topic = cls.__prepare_pubsub_topic_name()
        # according to the documentation,
        # the publish method is automatically retried.
        future = cls.publish_client.publish(topic, message.encode('utf-8'))
        try:
            return future.result()
        except Exception as e:
            logging.error(f"Publishing to topic {topic} has failed with "
                          f"message {message} with exception: {e}")
            raise e

    @classmethod
    def __construct_message(cls, prod_inference_url, prod_meta_url, timestamp):
        message = {
            'timestamp': timestamp,
            'environment': 'prod',
            # ideally we should have multiple stages and not harding this here.
            'dataset': 'taxonomy_b2c',
            'path': {
                'inferences': prod_inference_url,
                'metadata': prod_meta_url
            }
        }
        return json.dumps(message)

    @classmethod
    def __prepare_pubsub_topic_name(cls):
        project = os.getenv("GCP_PROJECT_ID")
        topic = os.getenv("INFERENCE_OUTPUT_PUBSUB_TOPIC")
        return f'projects/{project}/topics/{topic}'
