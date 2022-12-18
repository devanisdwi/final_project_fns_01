{{ config(materialized='view') }}

WITH dim__payment_type AS(
    SELECT DISTINCT
        payment_type
    FROM {{ ref('stg__fraud') }}
)

SELECT 
    (ROW_NUMBER() OVER(ORDER BY payment_type)) as id_payment_type, *
FROM 
    dim__payment_type