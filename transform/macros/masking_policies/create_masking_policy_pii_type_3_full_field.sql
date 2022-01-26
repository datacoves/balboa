{% macro create_masking_policy_pii_type_3_full_field(node_database,node_schema) %}

  create or replace masking policy {{node_database}}.{{node_schema}}.pii_type_3_full_field as (val string) 
    returns string ->
        case 
          when is_role_in_session('Z_POLICY_UNMASK_PII_TYPE_3') then val
          else '**********'
        end

{% endmacro %}