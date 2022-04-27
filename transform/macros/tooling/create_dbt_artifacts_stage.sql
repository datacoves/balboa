{# This macro creates an internal stage for dbt_artifacts #}
{#
    To run: 
    dbt run-operation create_dbt_artifacts_stage
#}

{%- macro create_dbt_artifacts_stage(db_nmae) -%}
  {% set sql %}
      create stage if not exists {{ target.database }}.source_dbt_artifacts.dbt_artifacts_stage
      file_format = ( type =  json );
  {% endset %}
  {% do run_query(sql) %}
  {{ log("Created DBT Artifacts Stage in database: " + target.database, info=true) }}
{%- endmacro -%}


