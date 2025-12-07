{{ config(materialized='table') }}

SELECT 
    country, 
    city, 
    longitude, 
    latitude
FROM {{ ref('stg_locations') }}