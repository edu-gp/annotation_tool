import base64
import json
import os
import uuid

import requests
from envparse import env
from google.cloud import secretmanager, storage

# configs
ALCHEMY_BUCKET = env('ALCHEMY_BUCKET')
INFERENCE_API = env('INFERENCE_API')
PROJECT_ID = env('PROJECT_ID')
API_TOKEN_NAME = env('API_TOKEN_NAME')


def handler(request):
    """Responds to an HTTP request from a pubsub owned by GDP.

    Args:
        request (flask.Request): HTTP request object.
    """
    request_json = request.get_json()
    message = None
    if request.args and "message" in request.args:
        message = request.args.get("message")
    elif request_json and "message" in request_json:
        message = request_json["message"]
    try:
        raw_data_str = base64.b64decode(message["data"]).decode("utf-8")
        data_dict = json.loads(raw_data_str)
        path = data_dict["path"]
        print(path)

        storage_client = storage.Client()

        source_bucket_name = path.split("/")[0]
        source_blob_name = path[len(source_bucket_name) + 1 :]
        print("Source bucket is " + source_blob_name)

        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_blob_name)

        destination_bucket_name = ALCHEMY_BUCKET
        print("Destination bucket is " + destination_bucket_name)
        destination_file_name = "_".join(source_blob_name.split("/")[1:])
        destination_blob_name = os.path.join("data", destination_file_name)

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

        url = INFERENCE_API
        print(f"URL to call is: {url}")
        payload = {
            "request_id": str(uuid.uuid1()),
            "dataset_name": destination_file_name,
        }
        print(f"Payload to the inference api: {payload}")

        secret_manager_client = create_gcp_client()
        api_token = get_secret(
            client=secret_manager_client,
            project_id=PROJECT_ID,
            secret_id=API_TOKEN_NAME,
        )
        headers = {
            "Authorization": "Bearer " + api_token,
            "Content-Type": "application/json",
        }
        print(json.dumps(payload))
        r = requests.post(url, json=payload, headers=headers)
    except Exception as e:
        print(e)
        raise e


def create_gcp_client():
    client = secretmanager.SecretManagerServiceClient()
    return client


def get_secret(client, project_id, secret_id, version_id="latest"):
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    return response.payload.data.decode("UTF-8")
