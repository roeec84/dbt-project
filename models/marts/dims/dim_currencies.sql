{{ config(materialized='table') }}

WITH currencies AS (
    SELECT DISTINCT target_currency AS currency_code
    FROM {{ref('stg_currency_rates')}}
)

SELECT
    currency_code,
    CASE currency_code
        WHEN 'EUR' THEN 'Euro'
        WHEN 'GBP' THEN 'British Pound'
        WHEN 'JPY' THEN 'Japanese Yen'
        WHEN 'AUD' THEN 'Australian Dollar'
        WHEN 'MXN' THEN 'Mexican Peso'
        WHEN 'ILS' THEN 'Shekel'
        ELSE currency_code
    END AS currency_name,
    
    CASE currency_code
        WHEN 'EUR' THEN '€'
        WHEN 'GBP' THEN '£'
        WHEN 'JPY' THEN '¥'
        WHEN 'AUD' THEN 'A$'
        WHEN 'MXN' THEN '$'
        WHEN 'ILS' THEN '₪'
        ELSE '$'
    END AS currency_symbol,
    CURRENT_TIMESTAMP() as updated_at
FROM currencies