{{ config(materialized='table') }}

SELECT
    product_id,
    name AS product_name,
    category,
    brand,
    department,
    sku,
    cost AS product_cost,
    retail_price,
    ROUND(retail_price - cost, 2) AS product_profit,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ref('stg_products')}}