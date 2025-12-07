{{ config(materialized='view') }}

WITH temp AS (SELECT
    DATE(date) as date,
    base_currency,
    target_currency,
    exchange_rate,
    inverse_rate_to_usd,
    CURRENT_TIMESTAMP() AS updated_at,
    ROW_NUMBER() OVER(PARTITION BY date, base_currency, target_currency ORDER BY date DESC) as rn
FROM {{source('api_data', 'currency_rates')}}
WHERE date IS NOT NULL and exchange_rate IS NOT NULL)

SELECT * FROM temp
WHERE rn = 1
ORDER BY date DESC