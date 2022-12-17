

  create or replace table `final-project-team1`.`timestamp_fraud_complete`.`dim_sender`
  
  
  OPTIONS()
  as (
    

WITH dim_sender AS(
    SELECT
    name_sender
    FROM `final-project-team1`.`timestamp_fraud_complete`.`stg_raw_fraud`
)

SELECT 
    *
FROM 
    dim_sender
  );
    