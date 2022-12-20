""" SECOND DAG FOR STREAMING DATA PIPELINE """
# https://github.com/fahrulrozim/final-project/tree/main/pubsub-stream
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/pubsub.html
import os
import csv
import json
import airflow
import logging
import json
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

PROJECT_ID = os.getenv("GCP_PROJECT")

AIRFLOW_DATA_PATH = '/home/airflow/gcs/data'
FILE_NAME = 'PS_20174392719_1491204439457_log'

TOPIC_NAME = 'coba-stream'
TOPIC_ID = f'projects/final-project-team1/topics/{TOPIC_NAME}'
# SUBS_NAME = 'coba-stream-sub'
SUBS_ID = f'projects/final-project-team1/subscriptions/{TOPIC_NAME}'

def push_messages():
    publisher = pubsub_v1.PublisherClient()

    with open(f'{AIRFLOW_DATA_PATH}/{FILE_NAME}.csv') as file:
        header = next(file) # skip the header (first row)
        csvreader = csv.reader(file)

        for row in itertools.islice(csvreader, 35):
            features = {
                "step": int(row[0]), 
                "type": str(row[1]), 
                "amount": float(row[2]), 
                "nameOrig": str(row[3]), 
                "oldbalanceOrg": float(row[4]), 
                "newbalanceOrig": float(row[5]), 
                "nameDest": str(row[6]), 
                "oldbalanceDest": float(row[7]), 
                "newbalanceDest": float(row[8]), 
                "isFraud": int(row[9]), 
                "isFlaggedFraud": int(row[10])
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
        bigquery.SchemaField('isFlaggedFraud', 'INTEGER')
    ]

    table_ref = bigquery.TableReference(dataset_bq, TABLE_NAME)
    table = bigquery.Table(table_ref, schema=schema)
    client_bq.create_table(table, exists_ok=True)
    table_id = f'{DATASET_NAME}.{TABLE_NAME}'
    return client_bq, table_id

def pull_messages():
    timeout = 5.0
    subscriber = pubsub_v1.SubscriberClient()
    client_bq, table_id = bq_api()

    def callback(message):
        # print(f'Received message: {message}')

        if message is not None:
            data = message.data.decode('utf-8')
            print(f'data: {message.data}')
            client_bq.insert_rows_from_dataframe(table_id, pd.DataFrame([data]))
            print('[INFO] Data Loaded to BigQuery')

        message.ack()

    stream_msg = subscriber.subscribe(SUBS_ID, callback=callback)
    print(f'Listening for messages on {SUBS_ID}')

    with subscriber: # Automate the response
        try:
            stream_msg.result() # see the messages
        except TimeoutError:
            stream_msg.cancel() # force
            stream_msg.result() 

################################## DAGS #########################################

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
    dagrun_timeout=timedelta(minutes=20),
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
        project_id=PROJECT_ID,
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

    pull_messages_task = PythonOperator(
        task_id="pull_messages_task",
        python_callable=pull_messages,
    )

    delete_topic_task = PubSubDeleteTopicOperator(
        task_id="delete_topic_task", 
        topic=TOPIC_NAME, 
        project_id=PROJECT_ID)

    delete_topic_task.trigger_rule = TriggerRule.ALL_DONE

    ######################################### Run the Dags ######################################################
    call_dataset_task >> create_topic_task >> push_messages_task
    call_dataset_task >> create_topic_task >> pull_messages_task

    push_messages_task >> delete_topic_task
    pull_messages_task >> delete_topic_task