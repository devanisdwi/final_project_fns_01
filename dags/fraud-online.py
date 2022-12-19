"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
# https://github.com/GoogleCloudPlatform/professional-services/blob/main/examples/dbt-on-cloud-composer/optimized/dag/dbt_with_kubernetes_optimized.py
import os
import airflow
import logging
import json
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

from airflow.models import Variable
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# from google.cloud import storage
# from google.oauth2 import service_account
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

# Select and use the correct Docker image from the private Google Cloud Repository (GCR)
IMAGE = 'gcr.io/{project}/dbt-builder-basic:latest'.format(
    project=project
)

# A Secret is an object that contains a small amount of sensitive data such as
# a password, a token, or a key. Such information might otherwise be put in a
# Pod specification or in an image; putting it in a Secret object allows for
# more control over how it is used, and reduces the risk of accidental
# exposure.
secret_volume = Secret(
    deploy_type='volume',
    # Path where we mount the secret as volume
    deploy_target='/var/secrets/google',
    # Name of Kubernetes Secret
    secret='dbt-sa-secret',
    # Key in the form of service account file name
    key='key.json'
)
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

    src_file = f'/home/airflow/gcs/data/{FILE_NAME}.csv'
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
# dbt default variables
# These variables will be passed into the dbt run
# Any variables defined here, can be used inside dbt

default_dbt_vars = {
        "project_id": project,
        # Example on using Cloud Composer's variable to be passed to dbt
        "bigquery_location": Variable.get("bigquery_location"),
        "key_file_dir": '/var/secrets/google/key.json',
        "source_data_project": Variable.get("source_data_project")
    }

# dbt default arguments
# These arguments will be used for running the dbt command

default_dbt_args = {
    # Setting the dbt variables
    "--vars": default_dbt_vars,
    # Define which target to load
    "--target": env,
    # Which directory to look in for the profiles.yml file.
    "--profiles-dir": ".dbt"
}

def get_dbt_full_args(dbt_args=None):
    """The function will return the dbt arguments.
    It should be called from an operator to get the execution date from Airflow macros"""
    # Add the execution date as variable in the dbt run
    dbt_full_vars = default_dbt_vars
    dbt_full_vars['execution_date'] = dbt_args['execution_date']

    # Specifcy which model should run
    dbt_full_args = default_dbt_args
    dbt_full_args['--models'] = dbt_args['model']

    # Converting the dbt_full_args into python list
    # The python list will be used for the dbt command
    # Example output ["--vars","{project_id: project}","--target","remote"]

    dbt_cli_args = []
    for key, value in dbt_full_args.items():
        dbt_cli_args.append(key)

        if isinstance(value, (list, dict)):
            value = json.dumps(value)

        # This part is to handle arguments with no value. e.g {"--store-failures": None}
        if value is not None:
            dbt_cli_args.append(value)

    return dbt_cli_args

# https://groups.google.com/g/cloud-composer-discuss/c/Lf0wa2ccI2c
def run_dbt_on_kubernetes(cmd=None, dbt_args=None, **context):
    """This function will execute the KubernetesPodOperator as an Airflow task"""
    dbt_full_args = get_dbt_full_args(dbt_args)

    execution_date = dbt_args['execution_date']

    # The pod id should be unique for each execution date
    pod_id = 'dbt_cli_{}_{}'.format(cmd, execution_date)
    KubernetesPodOperator(
        task_id=pod_id,
        name=pod_id,
        image_pull_policy='Always',
        arguments=[cmd] + dbt_full_args,
        namespace='default',
        service_account_name="default",
        get_logs=True,  # Capture logs from the pod
        log_events_on_failure=True,  # Capture and log events in case of pod failure
        is_delete_operator_pod=True, # To clean up the pod after runs
        image=IMAGE
        # secrets=[secret_volume]  # Set Kubernetes secret reference to dbt's service account JSON
    ).execute(context)
#################################################################################################

default_args = {
    "owner": "fastandseriouse",
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'fraud-online-pipeline',
    schedule_interval="@daily",
    default_args=default_args,
    description='End to End Online Fraud ELTL',
    dagrun_timeout=timedelta(minutes=20),
    max_active_runs=1,
    tags=['fns1-fp']
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

    dbt_run_staging = PythonOperator(
        task_id='dbt_run_staging',
        provide_context=True,
        python_callable=run_dbt_on_kubernetes,
        op_kwargs={
                "cmd": "run",
                "dbt_args":{"execution_date": DS,"model":"staging"}
            }
    )

    dbt_test_staging = PythonOperator(
        task_id='dbt_test_staging',
        provide_context=True,
        python_callable=run_dbt_on_kubernetes,
        op_kwargs={
                "cmd": "test",
                "dbt_args":{"execution_date": DS,"model":"staging","--store-failures": None}
            }
    )

    dbt_run_warehouse = PythonOperator(
        task_id='dbt_run_warehouse',
        provide_context=True,
        python_callable=run_dbt_on_kubernetes,
        op_kwargs={
                "cmd": "run",
                "dbt_args":{"execution_date": DS,"model":"warehouse"}
            }
    )

    dbt_test_warehouse = PythonOperator(
        task_id='dbt_test_warehouse',
        provide_context=True,
        python_callable=run_dbt_on_kubernetes,
        op_kwargs={
                "cmd": "test",
                "dbt_args":{"execution_date": DS,"model":"warehouse","--store-failures": None}
            }
    )

    ######################################### Run the Dags ######################################################
    # >> check_ls_task
    download_to_gcs_task >> unzip_dataset_task >> format_to_parquet_task >> create_empty_stg >> gcs_to_bigquery\
        >> dbt_run_staging >> dbt_test_staging >> dbt_run_warehouse > dbt_test_warehouse
    # >> dbt_test_staging >> dbt_test_warehouse