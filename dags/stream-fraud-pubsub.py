#!/usr/bin/python3
""" SECOND DAG FOR STREAMING DATA PIPELINE """
import os
import csv
import json
import airflow

import logging
import base64
import itertools
import pandas as pd

from curses.ascii import ACK
from concurrent.futures import TimeoutError
from time import sleep
from datetime import timedelta

from google.cloud import pubsub_v1
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.hooks.pubsub import PubSubHook
from airflow.providers.google.cloud.operators.pubsub import (
    PubSubCreateSubscriptionOperator,
    PubSubCreateTopicOperator,
    PubSubDeleteSubscriptionOperator,
    PubSubDeleteTopicOperator,
    PubSubPublishMessageOperator,
    PubSubPullOperator,
)
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
from airflow.utils.trigger_rule import TriggerRule

######################################### Variables ######################################################
PROJECT_ID = os.getenv("GCP_PROJECT")

AIRFLOW_DATA_PATH = '/home/airflow/gcs/data'
# FILE_NAME = 'PS_20174392719_1491204439457_log'
FILE_NAME = 'raw_fraud'

TOPIC_NAME = 'fraud-stream'
TOPIC_ID = f'projects/{PROJECT_ID}/topics/{TOPIC_NAME}'
SUBS_NAME = f'{TOPIC_NAME}-sub'
SUBS_ID = f'projects/{PROJECT_ID}/subscriptions/{SUBS_NAME}'
##########################################################################################################

def push_messages():
    publisher = pubsub_v1.PublisherClient()

    with open(f'{AIRFLOW_DATA_PATH}/{FILE_NAME}.csv') as file:
        header = next(file) # skip the header (first row)
        csvreader = csv.reader(file)

        for row in itertools.islice(csvreader, 35):
            features = {
                "transactionID": int(row[0]),
                "step": int(row[1]), 
                "type": str(row[2]), 
                "amount": float(row[3]), 
                "nameOrig": str(row[4]), 
                "oldbalanceOrg": float(row[5]), 
                "newbalanceOrig": float(row[6]), 
                "nameDest": str(row[7]), 
                "oldbalanceDest": float(row[8]), 
                "newbalanceDest": float(row[9]), 
                "isFraud": int(row[10]), 
                "isFlaggedFraud": int(row[11]),
                "timestamp": str(row[12])
            }
            try:
                attr_json = json.dumps(features)
                future = publisher.publish(TOPIC_ID, attr_json.encode('utf-8'))
            except Exception as e:
                print(f"Exception while producing record value - {features}: {e}")
            else:
                print(f"Successfully producing record value - {features}")

            print(f'published message id {future.result()} v')
            sleep(1)

def bq_api():
    DATASET_NAME = 'stream_pubsub'
    TABLE_NAME = 'fraud_online_stream'

    client_bq = bigquery.Client()
    client_bq.create_dataset(DATASET_NAME, exists_ok=True)
    dataset_bq = client_bq.dataset(DATASET_NAME)

    schema = [
        bigquery.SchemaField('transactionID', 'INTEGER'),
        bigquery.SchemaField('step', 'INTEGER'),
        bigquery.SchemaField('type', 'STRING'),
        bigquery.SchemaField('amount', 'FLOAT'),
        bigquery.SchemaField('nameOrig', 'STRING'),
        bigquery.SchemaField('oldbalanceOrg', 'FLOAT'),
        bigquery.SchemaField('newbalanceOrig', 'FLOAT'),
        bigquery.SchemaField('nameDest', 'STRING'),
        bigquery.SchemaField('oldbalanceDest', 'FLOAT'),
        bigquery.SchemaField('newbalanceDest', 'FLOAT'),
        bigquery.SchemaField('isFraud', 'INTEGER'),
        bigquery.SchemaField('isFlaggedFraud', 'INTEGER'),
        bigquery.SchemaField('timestamp', 'STRING'),
    ]

    table_ref = bigquery.TableReference(dataset_bq, TABLE_NAME)
    table = bigquery.Table(table_ref, schema=schema)
    client_bq.create_table(table, exists_ok=True)
    # table_id = f'{DATASET_NAME}.{TABLE_NAME}'
    return client_bq, table

def pull_messages():
    timeout = 5.0
    subs_client = pubsub_v1.SubscriberClient()
    client_bq, table = bq_api()
    data_row = []

    def callback(message):
        # print(f'Received message: {message}')
        data = message.data.decode('utf-8')
        print(f'data: {data}')
        client_bq.insert_rows_from_dataframe(table, pd.DataFrame([data]))
        print('[INFO] Data Loaded to BigQuery')
        # data_row.append(data)

        message.ack()

    stream_msg = subs_client.subscribe(SUBS_ID, callback=callback)
    print(f'Listening for messages on {SUBS_ID}')

    with subs_client: # Automate the response
        try:
            stream_msg.result() # see the messages
            # data_row.clear()
        except TimeoutError:
            stream_msg.cancel() # force
            stream_msg.result()
    # print(data_row)

def create_subscription(topic=TOPIC_NAME, subscription=SUBS_NAME):
    PubSubHook().create_subscription(topic=topic, subscription=subscription)

def delete_subscription(subs_name=SUBS_NAME):
    PubSubHook().delete_subscription(subscription=subs_name)

############################################ DAG #########################################################
default_args = {
    "owner": "fastandseriouse",
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# dagrun timeout to save same cost on resources
with DAG(
    'streaming-fraud-online',
    # schedule_interval="@daily",
    default_args=default_args,
    description='Streaming Online Fraud ELTL',
    dagrun_timeout=timedelta(minutes=5),
    max_active_runs=1,
    tags=['fns1-sf']
) as dag:

    call_dataset_task = BashOperator(
        task_id="call_dataset_task",
        bash_command=f'[ -f {AIRFLOW_DATA_PATH}/{FILE_NAME}.csv ] && echo "{FILE_NAME} exist." || echo "{FILE_NAME} does not exist."'
    )

    create_topic_task = PubSubCreateTopicOperator(
        task_id="create_topic_task",
        topic=TOPIC_NAME,
        fail_if_exists=False
    )

    push_messages_task = PythonOperator(
        task_id="push_messages_task",
        python_callable=push_messages,
    )

    # subscribe_task = PubSubCreateSubscriptionOperator(
    #     task_id="subscribe_task", 
    #     project_id=PROJECT_ID, 
    #     topic=TOPIC_NAME
    # )

    create_subscription_task = PythonOperator(
        task_id="create_subscription_task",
        python_callable=create_subscription,
    )

    pull_messages_task = PythonOperator(
        task_id="pull_messages_task",
        python_callable=pull_messages,
    )

    delete_subscription_task = PythonOperator(
        task_id="delete_subscription_task",
        python_callable=delete_subscription,
    )

    delete_topic_task = PubSubDeleteTopicOperator(
        task_id="delete_topic_task", 
        topic=TOPIC_NAME
    )

    delete_topic_task.trigger_rule = TriggerRule.ALL_DONE

    ######################################### Run the Dag ######################################################
    call_dataset_task >> create_topic_task >> create_subscription_task >> push_messages_task
    call_dataset_task >> create_topic_task >> create_subscription_task >> pull_messages_task

    pull_messages_task >> delete_subscription_task >> delete_topic_task
    push_messages_task >> delete_topic_task
    #############################################################################################################

# https://cloud.google.com/python/docs/reference/pubsub/latest
# https://github.com/fahrulrozim/final-project/tree/main/pubsub-stream
# https://medium.com/@montadhar/how-to-setup-cloud-composer-with-dbt-f9f657eb392a
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/pubsub.html