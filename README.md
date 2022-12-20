# Final Project FnS-01 IYKRA Data Fellowship 8 End to End Online Fraud Transactions.
 
*Prerequisites: [GCP Account](https://cloud.google.com/free-trial) with set-up billing (just in case).*

**How to Reproduce this:**
1. Go to Cloud Shell on GCP & clone this repo (you can also do it on-premise but it needs some set-up like GCloud SDK, Terraform, etc).
2. `cd terraform`, edit some variable on [var_over.sh]() (or variables.tf default value) then `terraform init; terraform plan; terraform apply;` wait for sometimes.
3. Edit some variable here: [batch-fraud-online.py]() on the part of bucket name (line 39).
4. Copy the relevant data by execute [start_copy.sh]().
5. Congrats! right now you can go to [Composer Service](https://console.cloud.google.com/composer) to check on the Airflow UI. Besides, you can also check the result of pipeline on every single services, GCS & BigQuery.

## Architecture ELTL Lambda

## Airflow Graph
### Streaming Fraud
### Batch Fraud

## DBT Docs

## Dashboard