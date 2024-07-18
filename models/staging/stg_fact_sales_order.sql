WITH fact_sales_order__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order_rename_column AS (
  SELECT 
    order_id AS sales_order_key,
    customer_id AS customer_key,
    picked_by_person_id AS picked_by_person_key,
    is_undersupply_backordered,
    expected_delivery_date,
    picking_completed_when,
    order_date
  FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
  SELECT 
    CAST(sales_order_key AS INTEGER) AS sales_order_key,
    CAST(customer_key AS INTEGER) AS customer_key,
    CAST(picked_by_person_key AS INTEGER) AS picked_by_person_key,
    CAST(is_undersupply_backordered AS BOOLEAN) AS is_undersupply_backordered_boolean,
    CAST(expected_delivery_date AS DATE) AS expected_delivery_date,
    CAST(picking_completed_when AS DATE) AS picking_completed_when,
    CAST(order_date AS DATE) AS order_date
  FROM fact_sales_order_rename_column
)

, fact_sales_order__convert_boolean AS (
  SELECT
    sales_order_key,
    customer_key,
    picked_by_person_key,
    CASE
      WHEN is_undersupply_backordered_boolean IS TRUE THEN 'Undersupply Backordered'
      WHEN is_undersupply_backordered_boolean IS FALSE THEN 'Not Undersupply Backordered'
      WHEN is_undersupply_backordered_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid' END
    AS is_undersupply_backordered,
    expected_delivery_date,
    picking_completed_when,
    order_date
  FROM fact_sales_order__cast_type
)

, fact_sales_order__add_undefined_record AS (
  SELECT
    sales_order_key,
    customer_key,
    picked_by_person_key,
    is_undersupply_backordered,
    expected_delivery_date,
    picking_completed_when,
    order_date
  FROM fact_sales_order__convert_boolean
  UNION ALL
  SELECT
    0 AS sales_order_key,
    0 AS customer_key,
    0 AS picked_by_person_key,
    'Undefined' AS is_undersupply_backordered,
    CAST('1900-01-01' AS DATE) AS expected_delivery_date,
    CAST('1900-01-01' AS DATE) AS picking_completed_when,
    CAST('1900-01-01' AS DATE) AS order_date
  UNION ALL
  SELECT
    -1 AS sales_order_key,
    -1 AS customer_key,
    -1 AS picked_by_person_key,
    'Invalid' AS is_undersupply_backordered,
    CAST('1900-01-01' AS DATE) AS expected_delivery_date,
    CAST('1900-01-01' AS DATE) AS picking_completed_when,
    CAST('1900-01-01' AS DATE) AS order_date
)

SELECT 
  sales_order_key,
  customer_key,
  picked_by_person_key,
  is_undersupply_backordered,
  expected_delivery_date,
  picking_completed_when,
  order_date
FROM fact_sales_order__add_undefined_record
