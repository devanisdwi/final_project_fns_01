{{ config(materialized='table') }}

WITH dim_recipient AS(
    SELECT
    name_recipient
    FROM {{ ref('stg_raw_fraud') }}
)

SELECT 
    *
FROM 
    dim_recipient