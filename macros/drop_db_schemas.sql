{# This macro drops all schemas in a given database #}
{#
    To run: 
    dbt run-operation drop_db_schemas --args {database: dev_db}
#}
{%- macro drop_db_schemas(db_name) -%}
  {% set all_schemas_in_db_sql %}
      select * from snowflake.account_usage.schemata
      where database = {{ db_name }}
  {% endset %}
  {% set all_schemas_in_db = run_query(all_schemas_in_db_sql).rows %}
  {% for this_schema in all_schemas_in_db %}
    {% set drop_this_schema_sql %}
      drop schema {{ db_name }}.{{ this_schema }}
    {% endset %}
    {% do run_query(drop_this_schema_sql) %}
  {% endfor %}

  {% do run_query(clone_db_sql) %}
{%- endmacro -%}