{{ config(materialized='table')}}


SELECT     
    transactionID as id_transaction,
    step,
    timestamp,
    type as payment_type,
    amount,
    nameOrig as name_sender,
    oldbalanceOrg as old_balance_sender,
    newbalanceOrig as new_balance_sender,
    nameDest as name_recipient,
    oldbalanceDest as old_balance_recipient,
    newbalanceDest as new_balance_recipient,
    {{ decode_isFraud('isFraud') }} as is_fraud,
    {{ decode_isFlaggedFraud('isFlaggedFraud') }} as is_flagged_fraud,
FROM {{ source('timestamp_fraud_complete', 'raw_fraud') }}
ORDER BY timestamp, step