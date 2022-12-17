

  create or replace table `final-project-team1`.`timestamp_fraud_complete`.`fact_tables`
  
  
  OPTIONS()
  as (
    

with fact_raw_fraud as (
    SELECT * 
    FROM `final-project-team1`.`timestamp_fraud_complete`.`stg_raw_fraud`
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
  );
    