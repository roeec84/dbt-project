{{ config(materialized='view') }}

SELECT
    order_id,
    user_id,
    status,
    gender,
    TIMESTAMP(created_at) AS created_at,
    DATE(created_at) AS order_date,
    TIMESTAMP(returned_at) AS returned_at,
    TIMESTAMP(shipped_at) AS shipped_at,
    TIMESTAMP(delivered_at) AS delivered_at,
    num_of_item,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ source('thelook', 'orders') }}
WHERE order_id IS NOT NULL AND user_id IS NOT NULL