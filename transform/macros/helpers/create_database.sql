{# This macro creates a database for use in pull request CI scripts. #}
{#
    To run: 
    dbt run-operation create_database
#}

{%- macro create_database() -%}
  {% set create_db_sql %}
      DROP DATABASE IF EXISTS {{ target.database }};
      CREATE DATABASE {{ target.database }};
  {% endset %}
  {% do run_query(create_db_sql) %}
  {{ log("Created Database: " ~ target.database, info=true) }}
{%- endmacro -%}
