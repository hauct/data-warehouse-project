SELECT
  person_key AS pickedbyperson_person_key
  , full_name AS pickedbyperson_person_name
  , search_name AS pickedbyperson_search_name
  , is_employee
FROM {{ ref('dim_person')}}