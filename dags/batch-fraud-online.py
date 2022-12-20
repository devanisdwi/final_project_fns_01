""" FIRST DAG FOR BATCHING DATA PIPELINE """
import os
import airflow
import logging
import json
from airflow import DAG
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
# https://stackoverflow.com/questions/55391105/location-of-home-airflow
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from datetime import timedelta
# Imports the Google Cloud client library
from google.cloud import storage

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as jsw
import pyarrow.parquet as pq

PROJECT_ID = 'final-project-team1'
BUCKET_NAME = 'us-west2-env-fns-test2-e8f06b0a-bucket'
AIRFLOW_DATA_PATH = '/home/airflow/gcs/data'
FILE_NAME = 'PS_20174392719_1491204439457_log'
DATASET_ID = 'dags_tests2_full'

# The environment variables from Cloud Composer
env = Variable.get("run_environment")
project = os.getenv("GCP_PROJECT")

# Airflow macro - Execution date
DS = '{{ ds }}'

#################################################################################################
# Functions to pass into dags
def format_to_parquet(src_file: str):
    """Convert CSV file to PARQUET file format"""
    if not src_file.endswith('.csv'):
        logging.error('Can only accept source files in CSV format, for the moment')
        return

    # storage_client = storage.Client()

    # bucket = storage_client.bucket(BUCKET_NAME)
    # blob = bucket.blob(f'{FILE_NAME}.csv')
    # blob.download_to_filename(f'{FILE_NAME}.csv')

    src_file = f'{AIRFLOW_DATA_PATH}/{FILE_NAME}.csv'
    # fs = gcsfs.GCSFileSystem(project='foo')
    # with fs.open("bucket/foo/bar.csv", 'rb') as csv_file:
    #     pv.read_csv(csv_file)
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket: str, object_name: str, local_file: str):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    * bucket: GCS bucket name (existed)
    * object_name: target path & file-name
    * local_file: source path & file-name\n
    -> return log
    """
    
    storage_client = storage.Client()

    buckt = storage_client.bucket(bucket)

    blob = buckt.blob(object_name)
    blob.upload_from_filename(local_file)

#################################################################################################

default_args = {
    "owner": "fastandseriouse",
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'dir': f'/home/airflow/gcs/dags/dbt',
    'profiles_dir' : f'/home/airflow/gcs/data/profiles'
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
        bash_command=f"kaggle datasets download rupakroy/online-payments-fraud-detection-dataset -p {AIRFLOW_DATA_PATH}"
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {AIRFLOW_DATA_PATH}/online-payments-fraud-detection-dataset.zip -d {AIRFLOW_DATA_PATH}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{AIRFLOW_DATA_PATH}/{FILE_NAME}.csv",
        },
    )

    create_empty_stg = BigQueryCreateEmptyDatasetOperator(
        task_id="create_empty_stg_dataset", 
        dataset_id=f'{DATASET_ID}'
    )

    gcs_to_bigquery = BigQueryCreateExternalTableOperator(
        task_id="gcs_to_bigquery_task",
        table_resource={
            "tableReference": {
                "projectId": f"{PROJECT_ID}",
                "datasetId": f"{DATASET_ID}",
                "tableId": "fraud_online_success_full_dag",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET_NAME}/data/{FILE_NAME}.parquet"],
            },
        },
    )

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
    download_to_gcs_task >> unzip_dataset_task >> format_to_parquet_task >> create_empty_stg >> gcs_to_bigquery\
        >> dbt_docs_gen

    download_to_gcs_task >> unzip_dataset_task >> format_to_parquet_task >> create_empty_stg >> gcs_to_bigquery\
        >> dbt_run_staging >> dbt_test_staging >> dbt_run_warehouse >> dbt_run_datamart
    #############################################################################################################