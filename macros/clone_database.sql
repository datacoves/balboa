{# This macro drops all contents of the TPDW_PREPROD schema, in order to rebuild it from scratch. #}
{#
    To run: 
    dbt run-operation clone_database --args {source_db: prod, target_db: dev}
#}
{%- macro clone_database(source_db, target_db) -%}
  {% set clone_db_sql %}
      DROP DATABASE IF EXISTS {{ target_db }};
      CREATE DATABASE {{ target_db }} CLONE {{ source_db }};
  {% endset %}
  {% do run_query(clone_db_sql) %}
{%- endmacro -%}