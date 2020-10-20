import logging

from google.cloud import pubsub, secretmanager


class GCPPubSubService:
    publish_client = pubsub.PublisherClient()

    @classmethod
    def publish_message(cls, project_id, topic_name, message_constructor, **kwargs):
        topic_path = f"projects/{project_id}/topics/{topic_name}"
        message = message_constructor(**kwargs)
        future = cls.publish_client.publish(topic_path, message.encode("utf-8"))
        try:
            logging.info(f"Published a message to topic {topic_path}: " f"{message}")
            return future.result()
        except Exception as e:
            logging.error(
                f"Publishing to topic {topic_path} has failed with "
                f"message {message} with exception: {e}"
            )
            raise e


class SecretManagerService:
    client = secretmanager.SecretManagerServiceClient()

    @classmethod
    def get_secret(cls, project_id, secret_id, version_id="latest"):
        name = cls.client.secret_version_path(project_id, secret_id, version_id)
        response = cls.client.access_secret_version(name)
        return response.payload.data.decode("UTF-8")
