{{ config(
    materialized='incremental',
    unique_key=['order_month', 'product_id'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH monthly_product_sales AS (
    SELECT
        DATE_TRUNC(DATE(created_at), MONTH) AS order_month,
        product_id,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(order_item_id) AS total_units_sold,
        COUNT(DISTINCT user_id) AS total_unique_buyers,
        SUM(sale_price) AS monthly_revenue,
        AVG(sale_price) AS avg_price
    FROM {{ref('stg_order_items')}}
    WHERE status NOT IN ('Returned', 'Cancelled')
    {% if is_incremental() %}
        AND DATE(created_at) >= DATE_SUB(
            (SELECT MAX(order_month) FROM {{ this }}),
            INTERVAL {{ var('interval_days', 3) }} + 30 DAY
        )
    {% endif %}
    GROUP BY 1, 2
),

previous_month_sales AS (
    SELECT
        *,
        LAG(monthly_revenue, 1, 0) OVER(PARTITION BY product_id ORDER BY order_month) AS previous_month_revenue,
        LAG(total_units_sold, 1, 0) OVER(PARTITION BY product_id ORDER BY order_month) AS previous_month_units_sold,
        LAG(total_unique_buyers, 1, 0) OVER(PARTITION BY product_id ORDER BY order_month) AS previous_month_buyers
    FROM monthly_product_sales
)

SELECT
    pm.order_month,
    FORMAT_DATE('%Y-%m', pm.order_month) AS year_month,
    EXTRACT(YEAR FROM pm.order_month) AS order_year,
    FORMAT_DATE('%B', pm.order_month) AS month_name,
    dp.product_id,
    dp.product_name,
    dp.category,
    dp.brand,
    dp.department,
    pm.total_orders,
    pm.total_units_sold,
    pm.total_unique_buyers,
    ROUND(pm.avg_price, 2) AS avg_month_price,
    ROUND(pm.monthly_revenue, 2) AS monthly_revenue,
    ROUND(pm.previous_month_revenue, 2) AS previous_month_revenue,
    pm.previous_month_units_sold,
    pm.previous_month_buyers,
    ROUND(pm.monthly_revenue - COALESCE(pm.previous_month_revenue, 0), 2) AS revenue_diff,
    ROUND(
        CASE
            WHEN pm.previous_month_revenue = 0 AND pm.monthly_revenue > 0 THEN 100
            ELSE SAFE_DIVIDE(pm.monthly_revenue - pm.previous_month_revenue, pm.previous_month_revenue) * 100
        END,
    2) AS revenue_growth_pct,
    pm.total_units_sold - COALESCE(pm.previous_month_units_sold, 0) AS units_sold_diff,
    ROUND(
        CASE
            WHEN pm.previous_month_units_sold = 0 AND pm.total_units_sold > 0 THEN 100
            ELSE SAFE_DIVIDE(pm.total_units_sold - pm.previous_month_units_sold, pm.previous_month_units_sold) * 100
        END,
    2) AS units_growth_pct,
    DENSE_RANK() OVER (PARTITION BY pm.order_month ORDER BY pm.total_units_sold DESC) AS trend_rank,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ref('dim_products')}} dp
JOIN previous_month_sales pm ON dp.product_id = pm.product_id