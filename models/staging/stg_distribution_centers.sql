{{ config(materialized='view') }}

SELECT
    id AS dist_id,
    name,
    latitude,
    longitude,
    distribution_center_geom,
    CURRENT_TIMESTAMP() AS updated_at
FROM {{source('thelook', 'distribution_centers')}}
WHERE id IS NOT NULL