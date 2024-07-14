WITH fact_sales_order__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__orders`
)

, fact_sales_order_rename_column AS (
  SELECT 
    order_id AS sales_order_key,
    customer_id AS customer_key,
    picked_by_person_id AS picked_by_person_key,
    order_date
  FROM fact_sales_order__source
)

, fact_sales_order__cast_type AS (
  SELECT 
    CAST(sales_order_key AS INTEGER) AS sales_order_key,
    CAST(customer_key AS INTEGER) AS customer_key,
    CAST(picked_by_person_key AS INTEGER) AS picked_by_person_key,
    CAST(order_date AS DATE) AS order_date
  FROM fact_sales_order_rename_column
)

SELECT 
  sales_order_key,
  customer_key,
  COALESCE(picked_by_person_key, 0) AS picked_by_person_key,
  order_date
FROM fact_sales_order__cast_type
