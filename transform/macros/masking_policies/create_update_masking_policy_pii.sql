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

{% macro create_update_masking_policy_pii(
        node_database,
        node_schema,
        data_type,
        mask_value,
        is_in_use=false )
%}

  {% if mask_value is string %}
    {% set mask_value = "\'" + mask_value + "\'"%}
  {% endif %}

  {% if (data_type | upper) == 'VARIANT' %}
    {% set a, b = mask_value %}
    {% set mask_value = "OBJECT_CONSTRUCT(\'" + a + "\',\'" + b + "\')" %}
    {{print(mask_value)}}
  {% endif %}


  {% set masking_policy_db = var("common_masking_policy_db") %}
  {% set masking_policy_schema = var("common_masking_policy_schema") %}

  {% set sql = '' %}

  {% set sql %}
    {% if is_in_use %}
        alter masking policy {{ masking_policy_db }}.{{ masking_policy_schema }}.masking_policy_pii_{{data_type}} set body  ->
    {% else %}
        create or replace masking policy {{ masking_policy_db }}.{{ masking_policy_schema }}.masking_policy_pii_{{data_type}} as (val {{data_type}})
            returns {{data_type}} ->
    {% endif %}
  {% endset %}

  {% set sql %}
    {{ sql }}
      case
        when is_role_in_session('z_policy_unmask_pii') then val
        else {{ mask_value }}
      end
  {% endset %}

  {{ return(sql) }}
{% endmacro %}
