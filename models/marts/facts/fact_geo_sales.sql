{{ config(
    materialized='incremental',
    unique_key=['order_month', 'country', 'city'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH monthly_geo_sales AS (
    SELECT
        DATE_TRUNC(DATE(oi.created_at), MONTH) AS order_month,
        dc.country,
        dc.city,
        COUNT(DISTINCT dc.customer_id) AS total_customers,
        COUNT(DISTINCT oi.order_id) AS total_orders,
        ROUND(SUM(oi.sale_price), 2) AS total_spent,
    FROM {{ref('stg_order_items')}} oi
    JOIN {{ref('dim_customers')}} dc
    ON oi.user_id = dc.customer_id
    WHERE oi.status NOT IN ('Returned', 'Cancelled')
    {% if is_incremental() %}
        AND DATE(oi.created_at) >= DATE_SUB(
            (SELECT MAX(order_month) FROM {{ this }}),
            INTERVAL {{ var('interval_dats', 3) }} + 30 DAY
        )
    {% endif %}
    GROUP BY 1, 2 ,3
),

country_monthly_totals AS (
    SELECT
        order_month,
        country,
        SUM(total_spent) AS country_total_spent
    FROM monthly_geo_sales
    GROUP BY 1, 2
)

SELECT
    mgs.order_month,
    FORMAT_DATE('%Y-%m', mgs.order_month) AS year_month,
    mgs.country,
    mgs.city,
    mgs.total_customers,
    mgs.total_orders,
    mgs.total_spent,
    ROUND(SAFE_DIVIDE(mgs.total_spent, mgs.total_orders), 2) AS avg_order_cost,
    ROUND(SAFE_DIVIDE(mgs.total_spent, mgs.total_customers), 2) AS avg_customer_spent,
    ROUND(SAFE_DIVIDE(mgs.total_orders, mgs.total_customers), 2) AS orders_per_customer,
    DENSE_RANK() OVER (PARTITION BY mgs.order_month ORDER BY cmt.country_total_spent DESC) AS country_rank, 
    DENSE_RANK() OVER (PARTITION BY mgs.order_month, mgs.country ORDER BY mgs.total_spent DESC) AS city_rank_in_country,
    CURRENT_TIMESTAMP() AS updated_at
FROM monthly_geo_sales mgs
JOIN country_monthly_totals cmt
ON mgs.order_month = cmt.order_month AND mgs.country = cmt.country