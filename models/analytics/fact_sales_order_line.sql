WITH fact_sales_order_lines__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_lines__rename_column AS (
  SELECT 
    order_line_id AS sales_order_line_key,
    order_id AS sales_order_key,
    stock_item_id AS product_key,
    quantity,
    unit_price,
    tax_rate,
    picking_completed_when
  FROM fact_sales_order_lines__source
)

, fact_sales__order_lines__cast_type AS (
  SELECT 
    CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key,
    CAST(sales_order_key AS INTEGER) AS sales_order_key,
    CAST(product_key AS INTEGER) AS product_key,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS NUMERIC) AS unit_price,
    CAST(tax_rate AS NUMERIC) AS tax_rate,
    CAST(picking_completed_when AS DATE) AS picking_completed_when
  FROM fact_sales_order_lines__rename_column
)

, fact_sales_order_lines__calculate_measure AS (
  SELECT 
    sales_order_line_key,
    sales_order_key,
    product_key,
    quantity,
    unit_price,
    quantity * unit_price AS gross_amount,
    tax_rate * quantity * unit_price AS tax_amount
    picking_completed_when,
  FROM fact_sales__order_lines__cast_type
)

SELECT 
  fact_line.sales_order_line_key,
  fact_line.sales_order_key,
  fact_line.product_key,
  COALESCE(fact_header.customer_key, -1) AS customer_key,
  COALESCE(fact_header.picked_by_person_key, -1) AS picked_by_person_key,
  fact_header.order_date,
  fact_line.quantity, 
  fact_line.unit_price,
  fact_line.gross_amount
FROM fact_sales_order_lines__calculate_measure AS fact_line
LEFT JOIN {{ref('stg_fact_sales_order')}} AS fact_header
ON fact_line.sales_order_key = fact_header.sales_order_key

