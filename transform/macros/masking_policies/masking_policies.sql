{# These macros create a masking policies #}
{#
    These policies are created using the macro:
    create_all_masking_policies

    To run:
    dbt run-operation create_masking_policy --args '{"resource_type": "sources"}' -t prd_pii
    dbt run-operation create_masking_policy --args '{"resource_type": "models"}' -t prd_pii

    To remove:
    dbt run-operation unapply_masking_policy --args '{"resource_type": "sources"}' -t prd_pii
    dbt run-operation unapply_masking_policy --args '{"resource_type": "models"}' -t prd_pii
#}

{# Policy for string fields #}
{% macro create_masking_policy_pii_full_field_string(node_database,node_schema) %}
  create or replace masking policy {{ node_database }}.{{ node_schema }}.masking_policy_pii_full_field_string as (val string)
    returns string ->
        case
          when is_role_in_session('z_policy_unmask_pii') then val
          else '**********'
        end
{% endmacro %}

{# Policy for string float fields #}
{% macro create_masking_policy_pii_full_field_float(node_database,node_schema) %}
  create or replace masking policy {{ node_database }}.{{ node_schema }}.masking_policy_pii_full_field_float as (val float)
    returns float ->
        case
          when is_role_in_session('z_policy_unmask_pii') then val
          else 0.00
        end
{% endmacro %}


{# Policy for string timestamp fields #}
{% macro create_masking_policy_pii_full_field_date(node_database,node_schema) %}
  create or replace masking policy {{ node_database }}.{{ node_schema }}.masking_policy_pii_full_field_date as (val timestamp_ntz)
    returns timestamp_ntz ->
        case
          when is_role_in_session('z_policy_unmask_pii') then val
          else date_from_parts(0001, 01, 01)::timestamp_ntz -- returns 0001-01-01 00:00:00.000
        end
{% endmacro %}
