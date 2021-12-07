{# This macro creates a database for use in pull request CI scripts. #}
{#
    To run: 
    dbt run-operation create_database --args '{target_db: demo_db2}'
#}

{%- macro create_database(target_db) -%}
  {% set create_db_sql %}
      DROP DATABASE IF EXISTS {{ target_db }};
      CREATE DATABASE {{ target_db }};
  {% endset %}
  {% do run_query(create_db_sql) %}
  {{ log("Created Database: " ~ target_db, info=true) }}
{%- endmacro -%}
