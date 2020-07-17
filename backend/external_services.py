import logging

from google.cloud import pubsub


class GCPPubSubService:
    publish_client = pubsub.PublisherClient()

    @classmethod
    def publish_message(cls, project_id, topic_name, message_constructor,
                        **kwargs):
        topic_path = f'projects/{project_id}/topics/{topic_name}'
        message = message_constructor(**kwargs)
        future = cls.publish_client.publish(topic_path, message.encode('utf-8'))
        try:
            return future.result()
        except Exception as e:
            logging.error(f"Publishing to topic {topic_path} has failed with "
                          f"message {message} with exception: {e}")
            raise e
