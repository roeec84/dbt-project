{{ config(materialized='view') }}

SELECT
    id AS user_id,
    first_name,
    last_name,
    LOWER(email) AS email,
    age,
    gender,
    state,
    street_address,
    postal_code,
    city,
    country,
    latitude,
    longitude,
    traffic_source,
    TIMESTAMP(created_at) AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{source('thelook', 'users')}}
WHERE id IS NOT NULL
    AND created_at IS NOT NULL