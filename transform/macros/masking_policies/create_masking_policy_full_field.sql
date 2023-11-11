{% macro create_masking_policy_full_field(node_database,node_schema) %}

CREATE MASKING POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.full_field AS (val string) 
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('ANALYST') THEN val 
           WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN SHA2(val)
      ELSE '**********'
      END

{% endmacro %}