{# This macro grants access to a database #}
{#
    To run:
    dbt run-operation grant_db_usage --args 'db_name: my_db'
#}

{%- macro grant_db_usage(db_name) -%}
    {% set db_usage_role_prefix = var("db_usage_role_prefix") %}

    {% set apply_db_grants_sql %}
        grant usage on database {{ db_name }} to role {{ db_usage_role_prefix }}{{ db_name }};
        grant usage on database {{ db_name }} to role useradmin;
    {% endset %}
    {% do run_query(apply_db_grants_sql) %}

    {{ log("Applied usage grant on Database: " ~ db_name, info=true) }}

{%- endmacro -%}
