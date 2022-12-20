{{
    config(
        materialized='view',
        partition_by={
            'field': 'timestamp',
            'data_type': 'timestamp',
            'granularity': 'hour'
        },
        cluster_by = 'payment_type'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact__tables') }}