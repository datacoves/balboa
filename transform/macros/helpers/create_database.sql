{# This macro creates a database for use in pull request CI scripts. #}
{#
    To run: 
    dbt run-operation create_database
#}

{%- macro create_database() -%}
  {% set create_db_sql %}
      use role z_create_database;
      create database if not exists {{ target.database }};
      grant ownership on database {{ target.database }} to role {{ target.role }};
      use role {{ target.role }};
  {% endset %}
  {% do run_query(create_db_sql) %}
  {{ log("Created Database: " ~ target.database, info=true) }}
{%- endmacro -%}
