{{ config(
    materialized='incremental',
    unique_key='order_date',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH daily_revenue AS (
    SELECT
        DATE(created_at) AS order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(sale_price) AS revenue_usd
    FROM {{ ref('stg_order_items') }}
    WHERE status NOT IN ('Cancelled', 'Returned')
    {% if is_incremental() %}
        AND DATE(created_at) >= DATE_SUB(
            (SELECT MAX(order_date) FROM {{ this }}),
            INTERVAL {{ var('interval_days', 3) }} DAY
        )
    {% endif %}
    GROUP BY 1
),

currency_conversion AS (
    SELECT
        rate_date,
        MAX(CASE WHEN currency_code = 'EUR' THEN exchange_rate END) AS usd_to_eur,
        MAX(CASE WHEN currency_code = 'GBP' THEN exchange_rate END) AS usd_to_gbp,
        MAX(CASE WHEN currency_code = 'JPY' THEN exchange_rate END) AS usd_to_jpy,
        MAX(CASE WHEN currency_code = 'AUD' THEN exchange_rate END) AS usd_to_aud,
        MAX(CASE WHEN currency_code = 'MXN' THEN exchange_rate END) AS usd_to_mxn,
        MAX(CASE WHEN currency_code = 'ILS' THEN exchange_rate END) AS usd_to_ils,
    FROM {{ ref('fact_currency_rates') }}
    GROUP BY 1
)

SELECT
    dr.order_date,
    FORMAT_DATE('%Y-%m', dr.order_date) AS year_month,
    EXTRACT(YEAR FROM dr.order_date) AS order_year,
    FORMAT_DATE('%A', dr.order_date) AS day_of_week,
    dr.total_orders,
    ROUND(dr.revenue_usd, 2) AS revenue_usd,
    ROUND(dr.revenue_usd * cc.usd_to_eur, 2) AS revenue_eur,
    ROUND(dr.revenue_usd * cc.usd_to_gbp, 2) AS revenue_gbp,
    ROUND(dr.revenue_usd * cc.usd_to_jpy, 2) AS revenue_jpy,
    ROUND(dr.revenue_usd * cc.usd_to_aud, 2) AS revenue_aud,
    ROUND(dr.revenue_usd * cc.usd_to_mxn, 2) AS revenue_mxn,
    ROUND(dr.revenue_usd * cc.usd_to_ils, 2) AS revenue_ils,
    CURRENT_TIMESTAMP() AS updated_at
FROM daily_revenue dr
LEFT JOIN currency_conversion cc
ON dr.order_date = cc.rate_date