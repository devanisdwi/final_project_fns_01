{{ config(materialized='table') }}

with fact_raw_fraud as (
    SELECT * 
    FROM {{ ref('stg_raw_fraud') }}
)

SELECT 
    id_transaction,
    step,
    timestamp,
    payment_type,
    amount,
    old_balance_sender,
    new_balance_sender,
    old_balance_recipient,
    new_balance_recipient,
    is_fraud,
    is_flagged_fraud
FROM
    fact_raw_fraud
