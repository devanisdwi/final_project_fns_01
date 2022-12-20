#!/usr/bin/python3
""" FIRST DAG FOR BATCHING DATA PIPELINE """
import os
import airflow
import logging
import json

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as jsw
import pyarrow.parquet as pq

from airflow import DAG
from datetime import timedelta
from google.cloud import bigquery
from google.cloud import storage
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

from airflow.models import Variable
from airflow_dbt.operators.dbt_operator import (
    DbtSeedOperator,
    DbtSnapshotOperator,
    DbtRunOperator,
    DbtTestOperator,
    DbtDocsGenerateOperator,
)

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

######################################### Variables ######################################################
PROJECT_ID = 'final-project-team1'
BUCKET_NAME = 'us-west2-env-fns-test2-e8f06b0a-bucket'
AIRFLOW_DATA_PATH = '/home/airflow/gcs/data'
# FILE_NAME = 'PS_20174392719_1491204439457_log'
FILE_NAME = 'raw_fraud'
# KAGGLE_USER_DATA = 'rupakroy/online-payments-fraud-detection-dataset'
KAGGLE_USER_DATA = 'devanisdwisutrisno/fraud-transactions-with-timestamp'
KAGGLE_DATASET = KAGGLE_USER_DATA.split('/')[1].strip()

DATASET_ID = 'final_project_data'
TABLE_NAME = 'raw_data'

# The environment variables from Cloud Composer
env = Variable.get("run_environment")
project = os.getenv("GCP_PROJECT")

# Airflow macro - Execution date
DS = '{{ ds }}'
##########################################################################################################
# Functions to pass into dags
def format_to_parquet(src_file: str):
    """Convert CSV file to PARQUET file format"""
    if not src_file.endswith('.csv'):
        logging.error('Can only accept source files in CSV format, for the moment')
        return

    client_bq = bigquery.Client(location='us-west2')
    client_bq.create_dataset(DATASET_ID, exists_ok=True)

    src_file = f'{AIRFLOW_DATA_PATH}/{FILE_NAME}.csv'
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

############################################ DAG #########################################################
default_args = {
    "owner": "fastandseriouse",
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'dir': f'/home/airflow/gcs/dags/dbt',
    'profiles_dir' : f'/home/airflow/gcs/dags/dbt/.dbt'
}

with DAG(
    'batch-fraud-online',
    # schedule_interval="@daily",
    default_args=default_args,
    description='Batch Online Fraud ELTL',
    dagrun_timeout=timedelta(minutes=5),
    max_active_runs=1,
    tags=['fns1-bf']
) as dag:

    download_to_gcs_task = BashOperator(
        task_id="download_to_gcs_task",
        bash_command=f"kaggle datasets download {KAGGLE_USER_DATA} -p {AIRFLOW_DATA_PATH}"
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {AIRFLOW_DATA_PATH}/{KAGGLE_DATASET}.zip -d {AIRFLOW_DATA_PATH}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_DATA_PATH}/{FILE_NAME}.csv",
        },
    )

    gcs_to_bigquery = BigQueryCreateExternalTableOperator(
        task_id="gcs_to_bigquery_task",
        table_resource={
            "tableReference": {
                "projectId": f"{PROJECT_ID}",
                "datasetId": f"{DATASET_ID}",
                "tableId": "raw_fraud",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET_NAME}/data/{FILE_NAME}.parquet"],
            },
        },
    )

    # gcs_to_bigquery = GCSToBigQueryOperator(
    #     task_id="gcs_to_bigquery_task",
    #     source_format='CSV',
    #     bucket=f"{BUCKET_NAME}",
    #     source_objects = [f"data/{FILE_NAME}.csv"],
    #     destination_project_dataset_table = f'{DATASET_ID}.{TABLE_NAME}',
    # )

    dbt_run_staging = DbtRunOperator(
        task_id='dbt_run_staging',
        select='staging',
    )
    
    dbt_test_staging  = DbtTestOperator(
        task_id='dbt_test_staging',
        select='staging',
        retries=0,  # Failing tests would fail the task, and we don't want Airflow to try again
    )

    dbt_run_warehouse = DbtRunOperator(
        task_id='dbt_run_warehouse',
        select='warehouse',
    )

    dbt_run_datamart = DbtRunOperator(
        task_id='dbt_run_datamart',
        select='datamart',
    )

    dbt_docs_gen = DbtDocsGenerateOperator(
        task_id='dbt_docs_gen',
    )

    ######################################### Run the Dag ######################################################
    download_to_gcs_task >> unzip_dataset_task >> format_to_parquet_task >> gcs_to_bigquery >> dbt_docs_gen

    download_to_gcs_task >> unzip_dataset_task >> format_to_parquet_task >> gcs_to_bigquery\
        >> dbt_run_staging >> dbt_test_staging >> dbt_run_warehouse >> dbt_run_datamart
    ############################################################################################################

# https://docs.astronomer.io/learn/airflow-dbt
# https://stackoverflow.com/questions/55391105/location-of-home-airflow
# https://groups.google.com/g/cloud-composer-discuss/c/Lf0wa2ccI2c
# https://github.com/GoogleCloudPlatform/professional-services/blob/main/examples/dbt-on-cloud-composer