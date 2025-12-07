{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge',
    partition_by={
        "field": "event_date",
        "data_type": "date",
        "granularity": "month"
    }
) }}

SELECT
    id AS event_id,
    user_id,
    sequence_number,
    session_id,
    LOWER(event_type) AS event_type,
    uri,
    browser,
    traffic_source,
    ip_address,
    city,
    state,
    postal_code,
    TIMESTAMP(created_at) AS created_at,
    DATE(created_at) AS event_date,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ source('thelook', 'events') }}
WHERE id IS NOT NULL
  AND user_id IS NOT NULL
  AND event_type IS NOT NULL
  AND session_id IS NOT NULL
{% if is_incremental() %}
  AND DATE(created_at) >= DATE_SUB(
      (SELECT MAX(event_date) FROM {{ this }}),
      INTERVAL {{ var('interval_days', 3) }} DAY
  )
{% endif %}