{{ config(materialized='table') }}

WITH dim_sender AS(
    SELECT
    name_sender
    FROM {{ ref('stg_raw_fraud') }}
)

SELECT 
    *
FROM 
    dim_sender