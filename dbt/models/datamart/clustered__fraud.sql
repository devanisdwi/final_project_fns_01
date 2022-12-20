{{
    config(
        materialized='view',
        partition_by={
            'field': 'timestamp',
            'data_type': 'timestamp',
            'granularity': 'hour'
        },
        cluster_by = 'is_fraud'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact__tables') }}