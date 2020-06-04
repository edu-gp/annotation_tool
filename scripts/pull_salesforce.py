#!/usr/bin/env python3

import logging
import os
from datetime import datetime
from typing import List, NoReturn

import typing
from simple_salesforce import Salesforce

import jwt
import time
import requests
import tldextract

from db.config import DevelopmentConfig
from db.model import Database, get_or_create, User, ClassificationAnnotation, \
    EntityTypeEnum, AnnotationSource, PredefinedUserName

from google.cloud import secretmanager


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")


def create_gcp_client():
    client = secretmanager.SecretManagerServiceClient()
    return client


def get_secret(client, project_id, secret_id, version_id='latest'):
    name = client.secret_version_path(project_id, secret_id, version_id)
    response = client.access_secret_version(name)
    return response.payload.data.decode('UTF-8')


def make_from_credentials(env: str) -> Salesforce:
    """Factory function for API instance using salesforce credentials
    Args:
        env: which environment is the script running on? Ideally we should
        have a beta stage and a prod stage.

    Returns:
        Instance of simple-salesforce object
    """

    ENV_SUFFIX_MAP = {
        'dev': '.dev',
        'beta': '.beta',
        'prod': ''
    }
    # ENV = 'prod'
    IS_SANDBOX = env != 'prod'

    gclient = create_gcp_client()

    consumer_key = get_secret(
        client=gclient,
        project_id=PROJECT_ID,
        secret_id="alchemy_salesforce_consumer_id")

    private_key = get_secret(
        client=gclient,
        project_id=PROJECT_ID,
        secret_id="alchemy_salesforce_private_key"
    )

    account_name = get_secret(
        client=gclient,
        project_id=PROJECT_ID,
        secret_id="alchemy_salesforce_email_account"
    )

    ISSUER = consumer_key
    SUBJECT = account_name + ENV_SUFFIX_MAP[env]

    DOMAIN = 'test' if IS_SANDBOX else 'login'

    print('Generating signed JWT assertion...')
    claim = {
        'iss': ISSUER,
        'exp': int(time.time()) + 300,
        'aud': f'https://{DOMAIN}.salesforce.com',
        'sub': SUBJECT,
    }
    assertion = jwt.encode(claim, private_key, algorithm='RS256',
                           headers={'alg': 'RS256'}).decode('utf8')

    print('Making OAuth request...')
    r = requests.post(f'https://{DOMAIN}.salesforce.com/services/oauth2/token',
                      data={
                          'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                          'assertion': assertion,
                      })

    response = r.json()
    print('Status:', r.status_code)
    print(response)

    return Salesforce(
        instance_url=response['instance_url'],
        session_id=response['access_token']
    )


def fetch_lead_status_and_dropoff_reason(api: Salesforce,
                                         start_time: datetime,
                                         end_time_exclusive: datetime) -> List:
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time_exclusive.strftime('%Y-%m-%dT%H:%M:%SZ')

    records = api.query_all(
        """
        SELECT Company, Company_Description__c, Website, Domain_Ext__c, 
        Dropoff_Reason__c, Id, Status 
        FROM Lead WHERE IsConverted = true 
        AND Domain_Ext__c <> Null
        AND CreatedDate >= {} 
        AND CreatedDate < {}
        """.format(start_time_str, end_time_str)
    )['records']
    return records


def _extract_domain(website_link: str) -> str:
    extraction_result = tldextract.extract(website_link)
    return extraction_result.domain + "." + extraction_result.suffix


def _is_b2c_related(record):
    return record["Dropoff_Reason__c"] == "Business Model - B2C" or \
           record["Status"] == "BD Accepted"


def _determine_value_for_b2c(record):
    if record["Status"] == "BD Accepted":
        value = -1
    elif record["Dropoff_Reason__c"] == "Business Model - B2C":
        value = 1
    else:
        value = 0  # This should never happen in theory.
    return value


def upsert_salesforce_b2c_annotations(
        dbsession,
        salesforce_user: User,
        last_dropoff_reasons: List,
        is_filtered_by: typing.Callable = _is_b2c_related) -> NoReturn:

    entities_selected = {}
    for record in last_dropoff_reasons:
        if is_filtered_by(record):
            if record["Domain_Ext__c"]:
                entities_selected[record["Domain_Ext__c"]] = record
            elif record["Website"]:
                entities_selected[_extract_domain(record["Website"])] = record
            else:
                logging.warning("Company {} had neither domain nor website "
                                "in Salesforce".format(record["Company"]))

    try:
        res = \
            dbsession.query(ClassificationAnnotation).filter(
                ClassificationAnnotation.entity.in_(entities_selected),
                ClassificationAnnotation.entity_type == EntityTypeEnum.COMPANY,
                ClassificationAnnotation.label == "B2C",
                ClassificationAnnotation.user_id == salesforce_bot.id
            ).all()
        existing_entity_annotations = {
            annotation.entity: annotation
            for annotation in res
        }

        new_entities = []
        updated_entities = []
        for entity in entities_selected:
            value = _determine_value_for_b2c(entities_selected[entity])
            if entity not in existing_entity_annotations:
                new_annotation = ClassificationAnnotation(
                    value=value,
                    entity=entity,
                    entity_type=EntityTypeEnum.COMPANY,
                    label="B2C",
                    user_id=salesforce_user.id,
                    context={
                        "text": entities_selected[entity][
                            "Company_Description__c"],
                        "meta": {
                            "name": entities_selected[entity]["Company"],
                            "domain": entity
                        }
                    },
                    source=AnnotationSource.SALESFORCE
                )
                new_entities.append(entity)
                dbsession.add(new_annotation)
            else:
                updated_entities.append(entity)
                # TODO I wonder if we should update the user to salesforce_bot.
                #  Or should we add another column `updated_by` so we know
                #  who made the change.
                existing_entity_annotations[entity].value = value

        logging.info("Inserting the following new entities:")
        logging.info(",".join(new_entities))
        logging.info("Updated the following existing entities:")
        logging.info(",".join(updated_entities))

        dbsession.commit()
    except Exception:
        dbsession.rollback()
        raise
    finally:
        dbsession.close()


if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    sf = make_from_credentials(env='prod')
    salesforce_records = fetch_lead_status_and_dropoff_reason(
        api=sf,
        start_time=datetime(year=2018, month=1, day=1),
        end_time_exclusive=datetime.now()
    )

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    salesforce_bot = get_or_create(
        dbsession=db.session,
        model=User,
        username=PredefinedUserName.SALESFORCE_BOT
    )
    upsert_salesforce_b2c_annotations(
        dbsession=db.session,
        salesforce_user=salesforce_bot,
        last_dropoff_reasons=salesforce_records
    )




