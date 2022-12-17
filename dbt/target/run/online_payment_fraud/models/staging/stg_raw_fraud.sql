

  create or replace table `final-project-team1`.`timestamp_fraud_complete`.`stg_raw_fraud`
  
  
  OPTIONS()
  as (
    


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
    case isFraud
        when 0 then "Not Fraud"
        when 1 then "Fraud"
    end as is_fraud,
    case isFlaggedFraud
        when 0 then "Not Fraud"
        when 1 then "Fraud"
    end as is_flagged_fraud,
FROM `final-project-team1`.`timestamp_fraud_complete`.`raw_fraud`
ORDER BY timestamp, step
  );
    