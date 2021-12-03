{# This macro drops and recreates in a given database #}
{#
    To run: 
    dbt run-operation drop_recreate_db --args '{db_name: demo_db2}'
#}

{%- macro drop_recreate_db(db_name) -%}
    {% set db_name = db_name | upper %}

    {% set drop_recreate_sql %}
        drop database if exists {{ db_name }};
        create database if not exists {{ db_name }};
    {% endset %}
    {{ log("Recreating Database: " ~ db_name, info=true) }}
    {% do run_query(drop_recreate_sql) %}

    {% set apply_grants_sql %}
        grant usage, create schema, monitor on database {{ db_name }} to analyst;
        grant usage on database {{ db_name }} to securityadmin;
    {% endset %}

    {{ log("Applying grants on Database: " ~ db_name, info=true) }}
    {% do run_query(apply_grants_sql) %}

{%- endmacro -%}