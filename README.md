# final_project_fns_01

IYKRA Data Fellowship 8 Final Project End to End Online Fraud Transactions Group1.
 
Prerequisites: GCP Account with set-up billing (just in case).

How to Reproduce this:
1. Go to Cloud Shell on GCP & clone this repo (you can also do it on-premise but it needs some set-up like GCloud SDK, Terraform, etc).
2. `cd terraform`, edit some variable on terraform/var.sh (or variables.tf) then `terraform init; terraform plan; terraform apply;` wait for sometimes.
3. Edit some variable here: batch-fraud-online.py on the part of bucket name (line 37).
4. Copy the relevant data.
``` 
    cd ..
    gcloud composer environments storage dags import \
    --environment env-fns-prod2 \
    --location us-west2 \
    --source="dbt"

    cd dags
    gcloud composer environments storage dags import \
    --environment env-fns-prod2 \
    --location us-west2 \
    --source="batch_fraud_online.py"

    gcloud composer environments storage dags import \
    --environment env-fns-prod2 \
    --location us-west2 \
    --source="stream_fraud_pubsub.py"
``` 
5. Congrats! right now you can go to [Composer Service](https://console.cloud.google.com/composer) to check on the Airflow UI. Besides, you can also check the result of pipeline on every single services, GCS & BigQuery.

## Architecture ELTL Lambda

## Airflow Graph
### Streaming Fraud
### Batch Fraud

## DBT Docs