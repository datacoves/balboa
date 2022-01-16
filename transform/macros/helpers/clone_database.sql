{# This macro drops a database and recreates it as a clone of another database. #}
{#
    To run: 
    dbt run-operation clone_database --args '{source_db: demo_db, target_db: demo_db2}'
#}

{%- macro clone_database(source_db, target_db) -%}
  {% set clone_db_sql %}
      DROP DATABASE IF EXISTS {{ target_db }};
      CREATE DATABASE {{ target_db }} CLONE {{ source_db }};
  {% endset %}
  {% do run_query(clone_db_sql) %}
  {{ log("Cloned Database: " ~ target_db ~ " from " ~ source_db, info=true) }}
{%- endmacro -%}
