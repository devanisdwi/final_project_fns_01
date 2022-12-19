{{
    config(
        materialized='view',
        partition_by={
            'field': 'timestamp',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact__tables') }}