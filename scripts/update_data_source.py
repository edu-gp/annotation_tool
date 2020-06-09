#!/usr/bin/env python3

import argparse
import logging
import os

import boto3

import pathlib
import gzip
import shutil
import tldextract

import json

from .pb_data import PitchbookData
from tqdm import tqdm
from datetime import datetime

from google.cloud import secretmanager
from google.cloud import storage

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def create_secret_manager_client():
    client = secretmanager.SecretManagerServiceClient()
    return client


def get_secret(client, project_id, secret_id, version_id='latest'):
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    return response.payload.data.decode('UTF-8')


def make_s3_client_from_credentials(gclient, env: str):
    """Factory function for AWS boto3 client instance for s3
    Args:
        env: which environment is the script running on? Ideally we should
        have a beta stage and a prod stage.

    Returns:
        Instance of boto3 client instance
    """

    ENV_SUFFIX_MAP = {
        'dev': '.dev',
        'beta': '.beta',
        'prod': ''
    }
    # ENV = 'prod'
    IS_SANDBOX = env != 'prod'

    aws_access_key = get_secret(
        client=gclient,
        project_id=PROJECT_ID,
        secret_id="aws_access_key")

    aws_secret_key = get_secret(
        client=gclient,
        project_id=PROJECT_ID,
        secret_id="aws_secret_key"
    )

    return boto3.client(
        's3',
        region_name="us-east-1",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )


def fetch_data_from_bucket(client, bucket_name, object_key, file_name):
    logging.info("Fetching S3 object at {}/{}".format(bucket_name, object_key))
    client.download_file(bucket_name, object_key, file_name)
    logging.info("Data saved at {}".format(file_name))


def parse_domain(website):
    if website is None:
        return None
    res = tldextract.extract(website.lower())
    return res.domain + '.' + res.suffix


def save_jsonl(fname, data):
    assert fname.endswith('.jsonl')
    with open(fname, 'w') as outfile:
        for entry in data:
            json.dump(entry, outfile)
            outfile.write('\n')


def find_latest_data_folder(s3, bucket_name):
    get_date = lambda obj: datetime.strptime(obj['Prefix'],
                                             '%Y-%m-%dT%H-%M-%S/')

    folders = []
    response = s3.list_objects_v2(Bucket=bucket_name, Delimiter='/')
    folders.extend(response['CommonPrefixes'])

    while response['IsTruncated']:
        next_continuation_token = response['NextContinuationToken']
        response = s3.list_objects_v2(Bucket=bucket_name,
                                      Delimiter='/',
                                      ContinuationToken=next_continuation_token)
        folders.extend(response['CommonPrefixes'])

    latest_folder = [folder['Prefix'] for folder in sorted(folders,
                                                           key=get_date,
                                                           reverse=True)][0]
    # Prefix has a trailing '/'
    return latest_folder[0:-1]


def upload_to_gcs(gcs_bucket, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    logging.info(
        "File {} uploaded to {}/{}.".format(
            source_file_name, gcs_bucket, destination_blob_name
        )
    )


if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    gclient = create_secret_manager_client()
    s3 = make_s3_client_from_credentials(gclient=gclient, env='prod')

    parser = argparse.ArgumentParser(description='Process S3 data pull '
                                                 'request.')
    parser.add_argument('--bucket_name', default=get_secret(
        gclient, PROJECT_ID, secret_id="spring3_data_bucket"))
    parser.add_argument('--folder_name', default=None)
    parser.add_argument('--object_name', default=get_secret(
        gclient, PROJECT_ID, secret_id="spring3_feature_data_filename"))
    args = parser.parse_args()

    bucket_name = args.bucket_name
    folder_name = args.folder_name
    object_name = args.object_name

    if folder_name is None:
        folder_name = find_latest_data_folder(s3, bucket_name)
        logging.info("Folder not specified. Fetching data from the latest "
                     "folder {}".format(folder_name))

    object_path = folder_name + "/" + object_name

    root_dir = os.getcwd()
    local_folder = f'{root_dir}/{bucket_name}/{folder_name}'

    pathlib.Path(local_folder).mkdir(parents=True, exist_ok=True)

    fetch_data_from_bucket(client=s3,
                           bucket_name=bucket_name,
                           object_key=object_path,
                           file_name=local_folder + "/" + object_name)

    filename_unzipped = object_name.replace(".gz", "")
    logging.info("Unzipping the {} to {}.".format(object_name,
                                                  filename_unzipped))
    with gzip.open(local_folder + "/" + object_name, 'rb') as f_in:
        with open(local_folder + "/" + filename_unzipped, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logging.info("File unzipped.")

    logging.info("Running Pitchbook data extraction...")
    base_cache_dir = f'{root_dir}/pb_cache_{folder_name}'
    pb = PitchbookData(
        f'{root_dir}/{bucket_name}/{folder_name}/{filename_unzipped}',
        base_cache_dir=base_cache_dir)

    records = []

    for snaps in tqdm(pb.companies_iterator(), total=pb.size()):
        name = snaps[-1]['data']['Company Name']
        domain = parse_domain(snaps[-1]['data']['Website'])
        desc = snaps[-1]['data']['Description']

        if domain and desc:
            records.append({
                'text': desc,
                'meta': {
                    'name': name,
                    'domain': domain
                }
            })
    logging.info("Data extraction completed.")

    extracted_filepath = f'{root_dir}/{bucket_name}/' \
                         f'{folder_name}/spring_{folder_name}.jsonl'
    save_jsonl(extracted_filepath, records)
    logging.info("Extracted data saved to {}".format(extracted_filepath))

    upload_to_gcs(gcs_bucket=get_secret(gclient, PROJECT_ID,
                                        secret_id="alchemy_data_bucket"),
                  source_file_name=extracted_filepath,
                  destination_blob_name=f'data/{folder_name}/spring'
                                        f'_{folder_name}.jsonl')





