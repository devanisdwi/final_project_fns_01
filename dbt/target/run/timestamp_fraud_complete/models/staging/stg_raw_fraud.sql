

  create or replace table `final-project-team1`.`timestamp_fraud_complete`.`stg_raw_fraud`
  
  
  OPTIONS()
  as (
    


SELECT * FROM `final-project-team1`.`timestamp_fraud_complete`.`raw_fraud`
ORDER BY timestamp, step
  );
    