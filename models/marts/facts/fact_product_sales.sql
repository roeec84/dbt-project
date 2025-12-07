{{config(
    materialized='table',
    on_schema_change='append_new_columns'
)}}

WITH product_sales AS (
    SELECT
        product_id,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(order_item_id) AS total_units_solds,
        COUNT(DISTINCT user_id) AS total_unique_buyers,
        SUM(sale_price) AS total_revenue,
        AVG(sale_price) AS avg_selling_price,
        DATE(MIN(created_at)) AS first_sale_date,
        DATE(MAX(created_at)) AS last_sale_date
    FROM {{ref('stg_order_items')}}
    WHERE status NOT IN ('Returned', 'Cancelled')
    GROUP BY 1
)

SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    dp.department,
    dp.product_cost,
    dp.retail_price,
    dp.product_profit,
    COALESCE(ps.total_orders, 0) AS total_orders,
    COALESCE(ps.total_units_solds, 0) AS total_units_solds,
    COALESCE(ps.total_unique_buyers, 0) AS total_unique_buyers,
    COALESCE(ps.total_revenue, 0) AS total_revenue,
    COALESCE(ps.avg_selling_price, 0) AS avg_selling_price,
    ps.first_sale_date,
    ps.last_sale_date,
    DATE_DIFF(CURRENT_DATE(), ps.last_sale_date, DAY) AS days_since_last_sale,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ref('dim_products')}} dp
JOIN product_sales ps ON dp.product_id = ps.product_id