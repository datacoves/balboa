{% macro empty_dev_schema(dry_run=true) %}

  {% set query %}
    select
      schema_name,
      ref_name,
      ref_type
    from (
      select
        table_schema as schema_name,
        table_name  as ref_name,
        split_part(table_type, ' ', -1)    as ref_type --allows for 'BASE TABLE' rather than 'TABLE' in results
      from information_schema.tables
      where table_schema = upper('{{ target.schema }}')
      )
  {% endset %}
  {%- set result = run_query(query) -%}
  {% if result %}
      {%- for to_delete in result -%}
        {%- if dry_run -%}
            {%- do log('to be dropped: ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], true) -%}
        {%- else -%}
            {%- do log('dropping ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], true) -%}
            {% set drop_command = 'drop ' ~ to_delete[2] ~ ' if exists ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ ' cascade;' %}
            {% do run_query(drop_command) %}
            {%- do log('dropped ' ~ to_delete[2] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1], true) -%}
        {%- endif -%}
      {%- endfor -%}
  {% else %}
    {% do log('no models to clean.', true) %}
  {% endif %}
{% endmacro %}