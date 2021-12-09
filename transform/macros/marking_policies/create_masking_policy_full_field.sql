{% macro create_masking_policy_full_field(node_database,node_schema) %}

create or replace masking policy {{node_database}}.{{node_schema}}.full_field as (val string) 
  returns string ->
      CASE WHEN CURRENT_ROLE() IN ('ANALYST') THEN val 
           WHEN CURRENT_ROLE() IN ('SYSADMIN') THEN SHA2(val)
      ELSE '**********'
      END
{% endmacro %}