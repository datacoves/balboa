{# This macro grants 'create database' permission to transformer_dbt_prd. #}
{#
    To run: 
    dbt run-operation grant_database_creation_to_dbt_prd
#}

{%- macro grant_database_creation_to_dbt_prd() -%}
  {% set grant_db_sql %}
    grant create database on account to role transformer_dbt_prd;
    grant role transformer_dbt_prd to user {{ target.user }};
  {% endset %}
  {% do run_query(grant_db_sql) %}
  {{ log("Granted 'create database' permission to transformer_dbt_prd", info=true) }}
{%- endmacro -%}
