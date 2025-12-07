{{ config(materialized='view') }}

SELECT
    id AS ii_id,
    product_id,
    TIMESTAMP(created_at) AS created_at,
    TIMESTAMP(sold_at) AS sold_at,
    ROUND(cost, 2) AS cost,
    product_category,
    product_name,
    product_brand,
    ROUND(product_retail_price, 2) AS product_retail_price,
    product_department,
    product_sku,
    product_distribution_center_id,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{source('thelook', 'inventory_items')}}
WHERE id IS NOT NULL