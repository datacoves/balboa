{# This macro drops and creates a database and is used for CI process #}
{#
    To run: 
    dbt run-operation create_custom_schema --args '{db_name: database, schema_name: schema}'
#}

{%- macro create_custom_schema(db_name, schema_name) -%}
    {% set db_name = db_name | upper %}
    {% set schema_name = schema_name | upper %}

    {% set db_create_sql %}
        create schema if not exists {{ db_name }}.{{ schema_name }};
    {% endset %}
    {{ log("Creating Schema: " ~ db_name ~ "." ~ schema_name, info=true) }}
    {% do run_query(db_create_sql) %}

{%- endmacro -%}