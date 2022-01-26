{# This macro grants access to a test database #}
{#
    To run: 
    dbt run-operation grant_prd_usage
#}

{%- macro grant_prd_usage() -%}
    {% set db_name = 'STAGING_PRD_COMMERCIAL_DW' %}

    {% set apply_db_grants_sql %}
        grant usage on database {{ db_name }} to role analyst;
        grant usage on database {{ db_name }} to role useradmin;
    {% endset %}
    {% do run_query(apply_db_grants_sql) %}

    {{ log("Applied usage grant on Database: " ~ db_name, info=true) }}

{%- endmacro -%}
