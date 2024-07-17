WITH dim_city AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__cities`
)

, dim_city_rename_column AS (
  SELECT 
    city_id AS city_key,
    city_name,
    state_province_id AS state_province_key
  FROM dim_city
)

, dim_city__cast_type AS (
  SELECT 
    CAST(city_key AS INTEGER) AS city_key,
    CAST(city_name AS STRING) AS city_name,
    CAST(state_province_key AS INTEGER) AS state_province_key
  FROM dim_city_rename_column
)

, dim_city__add_undefined_record AS (
  SELECT 
    city_key,
    city_name,
    state_province_key
  FROM dim_city__cast_type
  UNION ALL
  SELECT
    0 AS city_key,
    'Undefined' AS city_name,
    0 AS state_province_key
  UNION ALL
  SELECT
    -1 AS city_key,
    'Invalid' AS city_name,
    -1 AS state_province_key
)

SELECT 
  dim_city.city_key,
  dim_city.city_name,
  dim_city.state_province_key,
  COALESCE(dim_state_province.state_province_name, 'Invalid') AS state_province_name
FROM dim_city__add_undefined_record AS dim_city
LEFT JOIN {{ ref('stg_dim_state_province')}} AS dim_state_province
ON dim_city.state_province_key = dim_state_province.state_province_key

