{{ config(materialized='view') }}

SELECT 
    country, 
    city, 
    MAX(longitude) as longitude, 
    MAX(latitude) as latitude
FROM {{ source('thelook', 'users') }}
WHERE country IS NOT NULL AND city IS NOT NULL AND country <> 'null' AND city <> 'null'
GROUP BY 1, 2