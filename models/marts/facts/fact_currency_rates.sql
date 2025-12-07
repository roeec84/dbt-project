{{ config(
    materialized='incremental',
    unique_key=['rate_date', 'currency_code'],
    incremental_strategy='merge',
    on_schema_change='append_new_columns'
) }}

WITH daily_rates AS (
    SELECT
        date AS rate_date,
        target_currency AS currency_code,
        base_currency,
        CAST(exchange_rate AS NUMERIC) AS exchange_rate,
        CAST(inverse_rate_to_usd AS NUMERIC) AS inverse_rate
    FROM {{ref('stg_currency_rates')}}
    {% if is_incremental() %}
        WHERE date >= DATE_SUB(
            (SELECT MAX(rate_date) FROM {{ this }}),
            INTERVAL {{ var('interval_days', 3) }} DAY
        )
    {% endif %}
)

SELECT
    dr.rate_date,
    FORMAT_DATE('%Y-%m', dr.rate_date) AS year_month,
    EXTRACT(YEAR FROM dr.rate_date) AS rate_year,
    dr.currency_code,
    dr.base_currency,
    dc.currency_name,
    dc.currency_symbol,
    ROUND(dr.exchange_rate, 3) AS exchange_rate,
    ROUND(dr.inverse_rate, 3) AS inverse_rate,
    LAG(dr.exchange_rate) OVER (PARTITION BY dr.currency_code ORDER BY dr.rate_date) AS previous_day_rate,
    ROUND(dr.exchange_rate - LAG(dr.exchange_rate) OVER (PARTITION BY dr.currency_code ORDER BY dr.rate_date), 3) AS rate_change,
    ROUND(
        SAFE_DIVIDE(
            dr.exchange_rate - LAG(dr.exchange_rate) OVER (
                PARTITION BY dr.currency_code 
                ORDER BY dr.rate_date
            ),
            LAG(dr.exchange_rate) OVER (
                PARTITION BY dr.currency_code 
                ORDER BY dr.rate_date
            )
        ) * 100, 3
    ) AS rate_change_pct,
    CASE
        WHEN dr.exchange_rate > LAG(dr.exchange_rate) OVER (
            PARTITION BY dr.currency_code ORDER BY dr.rate_date
        ) THEN 'Strengthened'
        WHEN dr.exchange_rate < LAG(dr.exchange_rate) OVER (
            PARTITION BY dr.currency_code ORDER BY dr.rate_date
        ) THEN 'Weakened'
        ELSE 'Unchanged'
    END AS usd_direction,
    CURRENT_TIMESTAMP() AS updated_at
FROM daily_rates dr
LEFT JOIN {{ref('dim_currencies')}} dc
ON dr.currency_code = dc.currency_code