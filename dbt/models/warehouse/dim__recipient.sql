{{ config(materialized='view') }}

WITH dim__recipient AS (
    SELECT DISTINCT
        name_recipient
    FROM {{ ref('stg__fraud') }}
)

SELECT 
    (ROW_NUMBER() OVER(ORDER BY name_recipient)) as id_recipient, *
FROM 
    dim__recipient