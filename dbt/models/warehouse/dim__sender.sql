{{ config(materialized='view') }}

WITH dim__sender AS(
    SELECT DISTINCT
        name_sender
    FROM {{ ref('stg__fraud') }}
)

SELECT 
    (ROW_NUMBER() OVER(ORDER BY name_sender)) as id_sender, *
FROM 
    dim__sender