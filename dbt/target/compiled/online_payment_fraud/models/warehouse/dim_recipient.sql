

WITH dim_recipient AS(
    SELECT
    name_recipient
    FROM `final-project-team1`.`timestamp_fraud_complete`.`stg_raw_fraud`
)

SELECT 
    *
FROM 
    dim_recipient