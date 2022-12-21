#!/bin/bash
cd ..
gcloud composer environments storage dags import \
--environment composer-fns-prod2 \
--location us-west2 \
--source="dbt"

cd dags
gcloud composer environments storage dags import \
--environment composer-fns-prod2 \
--location us-west2 \
--source="batch_fraud_online.py"

gcloud composer environments storage dags import \
--environment composer-fns-prod2 \
--location us-west2 \
--source="stream_fraud_pubsub.py"