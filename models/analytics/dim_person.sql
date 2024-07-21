WITH dim_person__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.application__people`
)

, dim_person__rename_column AS (
  SELECT
    person_id AS person_key,
    full_name,
    search_name,
    is_employee,
    is_salesperson
  FROM dim_person__source
)

, dim_person__cast_type AS (
  SELECT
    CAST(person_key AS INTEGER)  AS person_key,
    CAST(full_name AS STRING) AS full_name,
    CAST(search_name AS STRING) AS search_name,
    CAST(is_employee AS BOOLEAN) AS is_employee_boolean,
    CAST(is_salesperson AS BOOLEAN) AS is_salesperson_boolean
  FROM dim_person__rename_column
)

, dim_person__convert_boolean AS (
  SELECT
    person_key,
    full_name,
    search_name,
    CASE
      WHEN is_employee_boolean IS TRUE THEN 'Employee'
      WHEN is_employee_boolean IS FALSE THEN 'Not Employee'
      WHEN is_employee_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid' END
    AS is_employee,
    CASE
      WHEN is_salesperson_boolean IS TRUE THEN 'Salesperson'
      WHEN is_salesperson_boolean IS FALSE THEN 'Not Salesperson'
      WHEN is_salesperson_boolean IS NULL THEN 'Undefined'
      ELSE 'Invalid' END
    AS is_salesperson
  FROM dim_person__cast_type
)

, dim_person_add_undefined_record AS (
  SELECT 
    person_key,
    full_name,
    search_name,
    is_employee,
    is_salesperson
  FROM dim_person__convert_boolean
  UNION ALL
  SELECT
    0 AS person_key,
    'Undefined' AS full_name,
    'Undefined' AS search_name,
    'Undefined' AS is_employee,
    'Undefined' AS is_salesperson
  UNION ALL
  SELECT
    -1 AS person_key,
    'Invalid' AS full_name,
    'Invalid' AS search_name,
    'Invalid' AS is_employee,
    'Invalid' AS is_salesperson
)

SELECT 
  dim_person.person_key,
  dim_person.full_name,
  dim_person.search_name,
  dim_person.is_employee,
  dim_person.is_salesperson,
FROM dim_person_add_undefined_record AS dim_person