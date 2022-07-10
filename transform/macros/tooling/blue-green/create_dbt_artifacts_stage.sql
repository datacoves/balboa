{# This macro creates an internal stage for dbt_artifacts #}
{#
    To run: 
    dbt run-operation create_dbt_artifacts_stage
#}

{# TODO: Make this generic so on blue/green all stages are recreated in clone, not just these two #}

{%- macro create_dbt_artifacts_stage(db_nmae) -%}
{# Artifacts is used for v1 #}
  {% set sql %}
      create stage if not exists {{ target.database }}.source_dbt_artifacts.artifacts
      file_format = ( type =  json );
  {% endset %}
  {% do run_query(sql) %}

{# dbt_artifacts_stage is used for v2 #}
  {% set sql %}
      create stage if not exists {{ target.database }}.source_dbt_artifacts.dbt_artifacts_stage
      file_format = ( type =  json );
  {% endset %}
  {% do run_query(sql) %}
  {{ log("Created DBT Artifacts Stage in database: " + target.database, info=true) }}
{%- endmacro -%}


