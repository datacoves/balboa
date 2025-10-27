{# This macro grants access to a test database #}
{#
    To run:
    dbt run-operation grant_prd_usage --args 'db_name: my_db'
#}

{%- macro grant_prd_usage(db_name) -%}

    {% set apply_db_grants_sql %}
        grant usage on database {{ db_name }} to role z_db__balboa;
        grant usage on database {{ db_name }} to role useradmin;
    {% endset %}
    {% do run_query(apply_db_grants_sql) %}

    {{ log("Applied usage grant on Database: " ~ db_name, info=true) }}

{%- endmacro -%}
