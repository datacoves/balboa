{# This macro resets environment for dbt 102 training. #}
{#
    To run: 
    dbt run-operation reset_for_dbt_102
#}

{%- macro reset_for_dbt_102() -%}
    {% set drop_schema_sql %}
        drop schema if exists {{ target.schema }};
    {% endset %}

    {% do run_query(drop_schema_sql) %}
    {{ log("Dropped Schema: " ~ target.schema, info=true) }}

{%- endmacro -%}
