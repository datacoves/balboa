{# This macro creates a database for use in pull request CI scripts. #}
{#
    To run: 
    dbt run-operation create_database
#}

{%- macro create_database() -%}
  {%- set database_exists = adapter.get_relation(
      database=target.database,
      schema="information_schema",
      identifier="tables") -%}
  {% if not database_exists %}
    {% set create_db_sql %}
        use role transformer_dbt_prd;
        create database {{ target.database }};
        grant ownership on database {{ target.database }} to role {{ target.role }};
        use role {{ target.role }};
    {% endset %}
    {% do run_query(create_db_sql) %}
    {{ log("Created Database: " ~ target.database, info=true) }}
  {% else %}
    {{ log("Database already exists: " ~ target.database, info=true) }}
  {% endif %}
{%- endmacro -%}
