{{ config(materialized='view') }}

SELECT
    id AS product_id,
    CAST(cost AS NUMERIC) AS cost,
    category,
    name,
    brand,
    CAST(retail_price AS NUMERIC) AS retail_price,
    department,
    sku,
    distribution_center_id,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{source('thelook', 'products')}}
WHERE id IS NOT NULL