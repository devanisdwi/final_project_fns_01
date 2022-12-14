"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

# from google.cloud import storage
# from google.oauth2 import service_account
# https://stackoverflow.com/questions/55391105/location-of-home-airflow
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from datetime import timedelta
# Imports the Google Cloud client library
from google.cloud import storage

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.json as jsw
import pyarrow.parquet as pq

PROJECT_ID = 'final-project-team1'
BUCKET_NAME = 'coba-manual1412'
FILE_NAME = 'PS_20174392719_1491204439457_log'

def format_to_parquet(src_file: str):
    """Convert CSV file to PARQUET file format"""
    if not src_file.endswith('.csv'):
        logging.error('Can only accept source files in CSV format, for the moment')
        return

    storage_client = storage.Client()

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{FILE_NAME}.csv')
    blob.download_to_filename(f'{FILE_NAME}.csv')

    src_file = f'{FILE_NAME}.csv'
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

default_args = {
    "owner": "fastandseriouse",
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'fraud-online-pipeline',
    schedule_interval="@daily",
    default_args=default_args,
    description='liveness monitoring dag',
    dagrun_timeout=timedelta(minutes=20),
    max_active_runs=1,
    tags=['fns1-fp']
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"kaggle datasets download rupakroy/online-payments-fraud-detection-dataset"
    )

    # check_ls_task = BashOperator(
    #     task_id="check_ls_task",
    #     bash_command="ls -lha"
    # )

    # pwd_task = BashOperator(
    #     task_id="pwd_task",
    #     bash_command="pwd"
    # )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip online-payments-fraud-detection-dataset.zip"
    )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"gs://coba-manual1412/PS_20174392719_1491204439457_log.csv",
    #     },
    # )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{FILE_NAME}.csv",
        },
    )

    container_to_gcs_task = PythonOperator(
        task_id="container_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET_NAME,
            "object_name": f"abc/PS_20174392719_1491204439457_log.parquet",
            "local_file": f"PS_20174392719_1491204439457_log.parquet",
        },
    )

    gcs_to_bigquery = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": "final-project-team1",
                "datasetId": "final_project_test",
                "tableId": "test_fraud_online",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://coba-manual1412/abc/PS_20174392719_1491204439457_log.parquet"],
            },
        },
    )

    # trigger_dbt_task = DbtCloudRunJobOperator(
    #     task_id="trigger_job_run1",
    #     job_id=48617,
    #     check_interval=10,
    #     timeout=300,
    # )

    # download_dataset_task 
    # >> check_ls_task
    download_dataset_task >> unzip_dataset_task >> format_to_parquet_task >> container_to_gcs_task >> gcs_to_bigquery 
    # >> trigger_dbt_task