{{ config(
    materialized='incremental',
    unique_key='event_date',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH user_events AS (
  SELECT
    user_id,
    DATE(created_at) AS event_date,
    event_type,
    COUNT(*) AS event_count
  FROM {{ref('stg_events')}}
  {% if is_incremental() %}
  WHERE DATE(created_at) >= DATE_SUB(
      (SELECT MAX(DATE(created_at)) FROM {{ this }}),
      INTERVAL {{ var('interval_days', 3) }} DAY
  )
  {% endif %}
  GROUP BY 1, 2, 3
),

user_events_counts AS (
  SELECT
    user_id,
    event_date,
    MAX(CASE WHEN event_type = 'home' THEN event_count ELSE 0 END) AS home_visits,
    MAX(CASE WHEN event_type = 'department' THEN event_count ELSE 0 END) AS department_views,
    MAX(CASE WHEN event_type = 'product' THEN event_count ELSE 0 END) AS product_views,
    MAX(CASE WHEN event_type = 'cart' THEN event_count ELSE 0 END) AS cart_adds,
    MAX(CASE WHEN event_type = 'purchase' THEN event_count ELSE 0 END) AS purchases
  FROM user_events
  GROUP BY 1, 2
),

daily_rates AS (
  SELECT
    event_date,
    COUNT(DISTINCT user_id) AS total_users,
    COUNT(DISTINCT CASE WHEN home_visits > 0 THEN user_id END) AS users_home,
    COUNT(DISTINCT CASE WHEN department_views > 0 THEN user_id END) AS users_department,
    COUNT(DISTINCT CASE WHEN product_views > 0 THEN user_id END) AS users_product,
    COUNT(DISTINCT CASE WHEN cart_adds > 0 THEN user_id END) AS users_cart,
    COUNT(DISTINCT CASE WHEN purchases > 0 THEN user_id END) AS users_purchase,
    SUM(home_visits) AS total_home_visits,
    SUM(product_views) AS total_product_views,
    SUM(cart_adds) AS total_cart_adds,
    SUM(purchases) AS total_purchases
  FROM user_events_counts
  GROUP BY 1
)

SELECT
  event_date,
  total_users,
  users_home,
  users_department,
  users_product,
  users_cart,
  users_purchase,
  ROUND(SAFE_DIVIDE(users_product, users_home) * 100, 2) AS home_to_product_rate,
  ROUND(SAFE_DIVIDE(users_cart, users_product) * 100, 2) AS product_to_cart_rate,
  ROUND(SAFE_DIVIDE(users_purchase, users_cart) * 100, 2) AS cart_to_purchase_rate,
  ROUND(SAFE_DIVIDE(users_purchase, users_home) * 100, 2) AS conversion_rate,
  total_home_visits,
  total_product_views,
  total_cart_adds,
  total_purchases
FROM daily_rates