import base64
import json
import os

from google.cloud import storage


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)
    try:
        data_dict = json.loads(pubsub_message)
        path = data_dict['path']
        print(path)

        storage_client = storage.Client()

        source_bucket_name = path.split("/")[0]
        source_blob_name = path[len(source_bucket_name) + 1:]

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        destination_bucket_name = 'alchemy-gp'
        destination_blob_name = os.path.join(
            'data', "_".join(source_blob_name.split("/")[1:]))

        destination_bucket = storage_client.bucket(destination_bucket_name)
        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )

        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

        # TODO Once the API is deployed, call the inference API.
    except Exception as e:
        print(e)
