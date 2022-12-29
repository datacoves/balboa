/*
  --add "{{ store_test_results(results) }}" to an on-run-end: block in dbt_project.yml
  --The next v.1.0.X release of dbt will include post run hooks for dbt test!
*/
{% macro store_test_results(results) %}
  {%- set test_results = [] -%}

  {%- for result in results if result.node.resource_type == 'test' -%}
    {%- set test_results = test_results.append(result) -%}
  {%- endfor -%}

  {% if test_results|length == 0 -%}
    {{ log("store_test_results found no test results to process.") if execute }}
    {{ return('') }}
  {% endif -%}

  {%- set central_tbl -%} {{ target.schema }}.test_results_central {%- endset -%}
  {%- set history_tbl -%} {{ target.schema }}.test_results_history {%- endset -%}

  {{ log("Centralizing " ~ test_results|length ~ " test results in " + central_tbl, info = true) if execute }}
  {{ log(test_results, info=true) }}
  create or replace table {{ central_tbl }} as (

  {%- for result in test_results %}

    {%- set test_name = '' -%}
    {%- set test_type = '' -%}
    {%- set column_name = '' -%}

    {%- if result.node.test_metadata is defined -%}
      {%- set test_name = result.node.test_metadata.name -%}
      {%- set test_type = 'generic' -%}

      {%- if test_name == 'relationships' -%}
        {%- set column_name = result.node.test_metadata.kwargs.field ~ ',' ~ result.node.test_metadata.kwargs.column_name -%}
      {%- else -%}
        {%- set column_name = result.node.test_metadata.kwargs.column_name -%}
      {%- endif -%}
    {%- elif result.node.name is defined -%}
      {%- set test_name = result.node.name -%}
      {%- set test_type = 'singular' -%}
    {%- endif %}

    select
      '{{ test_name }}'::text as test_name,
      '{{ result.node.config.severity }}'::text as test_severity_config,
      '{{ result.status }}'::text as test_result,
      '{{ process_refs(result.node.refs) }}'::text as model_refs,
      '{{ process_refs(result.node.sources, is_src=true) }}'::text as source_refs,
      '{{ column_name|escape }}'::text as column_names,
      '{{ result.node.name }}'::text as test_name_long,
      '{{ test_type }}'::text as test_type,
      '{{ result.execution_time }}'::text as execution_time_seconds,
      '{{ result.node.original_file_path }}'::text as file_test_defined,
      '{{ var("pipeline_name", "variable_not_set") }}'::text as pipeline_name,
      '{{ var("pipeline_type", "variable_not_set") }}'::text as pipeline_type,
      '{{ target.name }}'::text as dbt_cloud_target_name,
      '{{ env_var("DBT_CLOUD_PROJECT_ID", "manual") }}'::text as _audit_project_id,
      '{{ env_var("DBT_CLOUD_JOB_ID", "manual") }}'::text as _audit_job_id,
      '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}'::text as _audit_run_id,
      '{{ env_var("DBT_CLOUD_URL", "https://cloud.getdbt.com/#/accounts/account_id/projects/") }}'||_audit_project_id||'/runs/'||_audit_run_id::text as _audit_run_url,
      current_timestamp as _timestamp
    {{ "union all" if not loop.last }}

  {%- endfor %}

  );

  {% if target.name != 'default' %}
      create table if not exists {{ history_tbl }} as (
        select
          {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id,
          *
        from {{ central_tbl }}
        where false
      );

    insert into {{ history_tbl }}
      select
       {{ dbt_utils.generate_surrogate_key(["test_name", "test_result", "_timestamp"]) }} as sk_id,
       *
      from {{ central_tbl }}
    ;
  {% endif %}

{% endmacro %}


/*
  return a comma delimited string of the models or sources were related to the test.
    e.g. dim_customers,fct_orders

  behaviour changes slightly with the is_src flag because:
    - models come through as [['model'], ['model_b']]
    - srcs come through as [['source','table'], ['source_b','table_b']]
*/
{% macro process_refs( ref_list, is_src=false ) %}
  {% set refs = [] %}

  {% if ref_list is defined and ref_list|length > 0 %}
      {% for ref in ref_list %}
        {% if is_src %}
          {{ refs.append(ref|join('.')) }}
        {% else %}
          {{ refs.append(ref[0]) }}
        {% endif %}
      {% endfor %}

      {{ return(refs|join(',')) }}
  {% else %}
      {{ return('') }}
  {% endif %}
{% endmacro %}
