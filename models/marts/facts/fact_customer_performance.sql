{{ config(materialized='table') }}

WITH customer_metrics AS (
    SELECT
        user_id AS customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        ROUND(SUM(sale_price), 2) AS total_spent,
        (ROUND(SUM(sale_price), 2) / COUNT(DISTINCT order_id)) AS avg_order_cost,
        MIN(DATE(created_at)) AS first_order_date,
        MAX(DATE(created_at)) AS last_order_date,
        DATE_DIFF(CURRENT_DATE(), MAX(DATE(created_at)), DAY) AS days_from_last_order
    FROM {{ ref('stg_order_items') }}
    WHERE status NOT IN ('Returned', 'Cancelled')
    GROUP BY 1
)

SELECT
    dc.customer_id,
    dc.full_name,
    dc.email,
    dc.age_group,
    dc.gender,
    dc.country,
    dc.city,
    dc.registration_date,
    cm.total_orders,
    cm.total_spent,
    cm.avg_order_cost,
    cm.first_order_date,
    cm.last_order_date,
    cm.days_from_last_order,
    CASE WHEN cm.days_from_last_order <= 90 THEN TRUE ELSE FALSE END AS is_active,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{ ref('dim_customers') }} dc
JOIN customer_metrics cm ON dc.customer_id = cm.customer_id