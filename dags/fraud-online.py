"""A liveness prober dag for monitoring composer.googleapis.com/environment/healthy."""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
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

# priority_weight has type int in Airflow DB, uses the maximum.
    t1 = BashOperator(
        task_id='echo',
        bash_command='echo test',
        depends_on_past=False,
        priority_weight=2**31 - 1,
        do_xcom_push=False
    )

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"~/.local/bin/kaggle datasets download rupakroy/online-payments-fraud-detection-dataset -p gs://us-central1-env-fns-b17e3bcb-bucket//data"
    )

    # gcs_to_bigquery = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET_NAME}/raw/{PARQUET_FILE}"],
    #         },
    #     },
    # )

    # trigger_dbt_task = DbtCloudRunJobOperator(
    #     task_id="trigger_job_run1",
    #     job_id=48617,
    #     check_interval=10,
    #     timeout=300,
    # )

    download_dataset_task 
    # >> gcs_to_bigquery >> trigger_dbt_task