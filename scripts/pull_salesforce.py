#!/usr/bin/env python3

import logging
from datetime import datetime
from typing import List, NoReturn

from simple_salesforce import Salesforce

import jwt
import time
import requests

from db.config import DevelopmentConfig
from db.model import Database, get_or_create, User, ClassificationAnnotation, \
    EntityTypeEnum


def make_from_credentials(env: str) -> Salesforce:
    """Factory function for API instance using salesforce credentials
    Args:
        username: salesforce login (email)
        password: salesforce password
        security_token: salesforce security token
        is_sandbox: are we connecting to a sandbox instance?

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

    consumer_key = "Please ask on how to get it."
    KEY_FILE = 'Please ask on how to get it.'
    ISSUER = consumer_key
    SUBJECT = 'Please ask on how to get it' + ENV_SUFFIX_MAP[env]

    DOMAIN = 'test' if IS_SANDBOX else 'login'

    print('Loading private key...')
    with open(KEY_FILE) as fd:
        private_key = fd.read()

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


def fetch_last_dropoff_reason(api: Salesforce,
                              start_time: datetime,
                              end_time_exclusive: datetime) -> List:
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time_exclusive.strftime('%Y-%m-%dT%H:%M:%SZ')
    # TODO confirm what are all the fields we need?
    # TODO add another query based on Diego's note.
    last_dropoff_reason = api.query_all(
        """
        SELECT cbit__ClearbitDomain__c, CreatedDate, Last_Dropoff_Date__c,
        Last_Dropoff_Reason__c, Name, Website 
        FROM Account 
        WHERE CreatedDate >= {} 
        AND CreatedDate < {}
        """.format(start_time_str, end_time_str)
    )
    return last_dropoff_reason["records"]


# TODO do we need the time range for the lead?
def fetch_lead_status(api: Salesforce,
                      start_time: datetime,
                      end_time_exclusive: datetime) -> List:
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    end_time_str = end_time_exclusive.strftime('%Y-%m-%dT%H:%M:%SZ')

    companies_with_bd_accepted = api.query_all(
        """
        SELECT Lead.Company FROM Lead WHERE 
        Lead.Status = 'BD Accepted' Limit 10
        """
    )["records"]

    selected_companies = [
        company['Company'] for company in companies_with_bd_accepted
    ]

    selected_companies_sql_condition = ','.join([
        "'{}'".format(company) for company in selected_companies
    ])

    result = api.query_all(
        """
        SELECT Company_Name__c, cbit__ClearbitDomain__c FROM Account WHERE
        Account.Company_Name__c IN ({}) 
        AND CreatedDate >= {} 
        AND CreatedDate < {}
        """.format(selected_companies_sql_condition,
                   start_time_str,
                   end_time_str)
    )
    return result['records']


# TODO are we ever going to unmark an entity from B2C to non-B2C by
#  salesforce? If no, then there is no point of update existing entries.
def insert_salesforce_b2c_annotations(
        dbsession,
        salesforce_user: User,
        last_dropoff_reasons: List,
        filter_by: str = "Business Model - B2C") -> NoReturn:
    entities_dropped_due_to_b2c = [
        record["cbit__ClearbitDomain__c"]
        for record in last_dropoff_reasons if record[
            "Last_Dropoff_Reason__c"] == filter_by
    ]

    logging.info(entities_dropped_due_to_b2c)

    """
    Format of a context:
    {"text": "Developer of a smart sleep mask intended to take control of sleep schedule. The company's mask uses light flash technology to treat circadian rhythm disruptions, such as jet lag, delayed phase sleep disorder (DPSD) and seasonal affective disorder (SAD), enabling users to get a good sleep.", "meta": {"name": "LumosTech", "domain": "lumos.tech"}}
    """
    try:
        res = \
            dbsession.query(ClassificationAnnotation.entity).filter(
                ClassificationAnnotation.entity.in_(entities_dropped_due_to_b2c),
                ClassificationAnnotation.entity_type == EntityTypeEnum.COMPANY,
                ClassificationAnnotation.label == "B2C",
                ClassificationAnnotation.user_id == salesforce_bot.id
            ).all()
        exisiting_entity = set([
            item[0] for item in res
        ])
        annotations_to_add = [
            ClassificationAnnotation(
                value=1,
                entity=entity,
                entity_type=EntityTypeEnum.COMPANY,
                label="B2C",
                user_id=salesforce_user.id,
                context="Extracted from Salesforce"
            )
            for entity in entities_dropped_due_to_b2c
            if entity not in exisiting_entity
        ]
        print("Inserting the following new entities:")
        new_entities = [annotation.entity for annotation in annotations_to_add]
        logging.info(",".join(new_entities))
        dbsession.add_all(annotations_to_add)
        dbsession.commit()
    except Exception:
        dbsession.rollback()
        raise


if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)

    sf = make_from_credentials(env='prod')
    records = fetch_last_dropoff_reason(
        api=sf,
        start_time=datetime(year=2018, month=1, day=1),
        end_time_exclusive=datetime(year=2021, month=1, day=1)
    )
    print(records)

    db = Database(DevelopmentConfig.SQLALCHEMY_DATABASE_URI)
    salesforce_bot = get_or_create(
        dbsession=db.session,
        model=User,
        username="salesforce_bot"
    )
    insert_salesforce_b2c_annotations(
        dbsession=db.session,
        salesforce_user=salesforce_bot,
        last_dropoff_reasons=records
    )




