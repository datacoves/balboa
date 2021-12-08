{# This macro grants access to the current database to role z_pr_databases. #}
{#
    To run: 
    dbt run-operation grant_access_to_pr_database
#}

{%- macro grant_access_to_pr_database() -%}
  {% set grant_db_sql %}
      grant usage on database {{ target.database }} to role z_pr_databases;
      grant usage on future schemas in database {{ target.database }} to role z_pr_databases;
  {% endset %}
  {% do run_query(grant_db_sql) %}
  {{ log("Created Database: " ~ target.database, info=true) }}
{%- endmacro -%}
