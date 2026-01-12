{{ config(
    materialized='table',
    on_schema_change='append_new_columns'
) }}

SELECT
  DATE(oi.created_at) AS order_date,
  u.country as country,
  COUNT(DISTINCT oi.order_id) AS total_orders,
  COUNT(oi.order_item_id) AS total_items_sold,
  ROUND(SUM(oi.sale_price), 2) AS daily_revenue,
  ROUND(AVG(oi.sale_price), 2) AS avg_item_price,
  (ROUND(SUM(oi.sale_price) / COUNT(DISTINCT oi.order_id), 2)) as aov
FROM {{ref('stg_order_items')}} oi
JOIN {{ref('stg_users')}} u
ON u.user_id = oi.user_id
GROUP BY DATE(oi.created_at), u.country