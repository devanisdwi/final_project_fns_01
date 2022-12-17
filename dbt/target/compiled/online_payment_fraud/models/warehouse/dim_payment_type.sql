

WITH dim_payment_type AS(
    SELECT 
    payment_type
    -- payment_type
    -- case payment_type
    --     when payment_type "CASH_IN" THEN 1
    --     when payment_type "CASH_OUT" THEN 2
    --     when payment_type "DEBIT" THEN 3
    --     when payment_type "PAYMENT" THEN 4
    -- end as id_payment_type
    FROM `final-project-team1`.`timestamp_fraud_complete`.`stg_raw_fraud`
)

SELECT 
    *
FROM 
    dim_payment_type