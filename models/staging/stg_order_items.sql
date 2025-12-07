{{ config(materialized='view') }}

SELECT
    id AS order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    TIMESTAMP(created_at) AS created_at,
    TIMESTAMP(shipped_at) AS shipped_at,
    TIMESTAMP(delivered_at) AS delivered_at,
    TIMESTAMP(returned_at) AS returned_at,
    ROUND(sale_price, 2) AS sale_price,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ source('thelook', 'order_items') }}
WHERE id IS NOT NULL
AND created_at IS NOT NULL