import logging
import uuid

import requests
from envparse import env

from alchemy.cloud_function.inference import inference_api_invoker


def get_api_token(project_id, token_name):
    secret_manager_client = inference_api_invoker.create_gcp_client()
    api_token = inference_api_invoker.get_secret(
        client=secret_manager_client,
        project_id=project_id,
        secret_id=token_name,
    )
    return api_token


def send_request(dataset_name, api_token):
    request_id = str(uuid.uuid1())
    payload = {
        "request_id": request_id,
        "dataset_name": dataset_name,
    }
    headers = {
        "Authorization": "Bearer " + api_token,
        "Content-Type": "application/json",
    }
    logging.info(f"Sending request, ID = {request_id}")
    r = requests.post(url, json=payload, headers=headers)
    return r


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Invoke inference")
    parser.add_argument("dataset", help="name of dataset file")
    parser.add_argument("-u", "--url", required=False, help="Inference API url")
    parser.add_argument("--token-from-env", action="store_true", required=False)
    parser.add_argument("-p", required=False, help="GCP project ID")
    parser.add_argument("-t", "--token-name", required=False, help="API token name on GCP secret manager.")

    args = parser.parse_args()

    logging.root.setLevel(logging.INFO)

    url = args.url or env('INFERENCE_API')
    project_id = args.p or env("GCP_PROJECT_ID")
    dataset_name = args.dataset

    if args.token_from_env:
        api_token = env('API_TOKEN')
    else:
        token_name = args.token_name or env('API_TOKEN_NAME')
        api_token = get_api_token(project_id, token_name)
        logging.info("Provisioned API token")
    response = send_request(dataset_name, api_token)
    logging.info(f"Response code = {response.status_code}")
    logging.info(f"Response = {response.text}")

