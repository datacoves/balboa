{# These macros create a masking policies #}
{#
    To run: 
    dbt run-operation create_masking_policy --args '{"resource_type": "sources"}'
    dbt run-operation create_masking_policy --args '{"resource_type": "models"}'

    To remove:
    dbt run-operation unapply_masking_policy --args '{"resource_type": "sources"}'
    dbt run-operation unapply_masking_policy --args '{"resource_type": "models"}'
#}

{# Policy for string fields #}
{% macro create_masking_policy_pii_type_2_full_field(node_database,node_schema) %}
  create or replace masking policy {{node_database}}.{{node_schema}}.pii_type_2_full_field as (val string) 
    returns string ->
        case 
          when is_role_in_session('Z_POLICY_UNMASK_PII_TYPE_2') then val
          else '**********'
        end
{% endmacro %}

{# Policy for string timestamp fields #}
{% macro create_masking_policy_pii_type_2_full_field_date(node_database,node_schema) %}
  create or replace masking policy {{node_database}}.{{node_schema}}.pii_type_2_full_field_date as (val TIMESTAMP_NTZ) 
    returns TIMESTAMP_NTZ ->
        case 
          when is_role_in_session('Z_POLICY_UNMASK_PII_TYPE_2') then val
          else date_from_parts(0001, 01, 01)::timestamp_ntz -- returns 0001-01-01 00:00:00.000
        end
{% endmacro %}
