WITH dim_supplier__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.purchasing__suppliers`
)

, dim_supplier__rename_column AS (
  SELECT 
    supplier_id  AS supplier_key,
    supplier_name,
    supplier_category_id AS supplier_category_key,
    delivery_method_id AS delivery_method_key
  FROM dim_supplier__source
)

, dim_supplier__cast_type AS (
  SELECT 
    CAST(supplier_key AS INTEGER)  AS supplier_key,
    CAST(supplier_name AS STRING) AS supplier_name,
    CAST(supplier_category_key AS INTEGER)  AS supplier_category_key,
    CAST(delivery_method_key AS INTEGER)  AS delivery_method_key,
  FROM dim_supplier__rename_column
 )

, dim_supplier__add_undefined_record AS (
  SELECT 
    supplier_key,
    supplier_name,
    supplier_category_key,
    delivery_method_key
  FROM dim_supplier__cast_type
  UNION ALL
  SELECT
    0 AS supplier_key,
    'Undefined' AS supplier_name,
    0 AS supplier_category_key,
    0 AS delivery_method_key
  UNION ALL
  SELECT
    -1 AS supplier_key,
    'Invalid' AS supplier_name,
    -1 AS supplier_category_key,
    -1 AS delivery_method_key
)

SELECT
  dim_supplier.supplier_key,
  dim_supplier.supplier_name,
  dim_supplier.supplier_category_key,
  COALESCE(dim_supplier_category.supplier_category_name, 'Invalid') AS supplier_category_name,
  dim_supplier.delivery_method_key,
  COALESCE(dim_delivery_method.delivery_method_name, 'Invalid') AS delivery_method_name
FROM dim_supplier__add_undefined_record AS dim_supplier
LEFT JOIN {{ref('stg_dim_supplier_category')}} AS dim_supplier_category
ON dim_supplier.supplier_category_key = dim_supplier_category.supplier_category_key
LEFT JOIN {{ref('stg_dim_delivery_method')}} AS dim_delivery_method
ON dim_supplier.delivery_method_key = dim_delivery_method.delivery_method_key

