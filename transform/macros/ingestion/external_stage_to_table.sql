{# This macro ingests external sources like S3 files into Snowflake #}
{#
    To run:
    dbt run-operation ingest_external_table --args 'external_source_name: my_table_on_S3'
#}


{% macro ingest_external_table(external_source_name) %}
    {#
    Inspired by dbt-external-tables macro
    https://github.com/dbt-labs/dbt-external-tables/blob/main/macros/plugins/snowflake/snowpipe/get_copy_sql.sql
    #}

    {% set target_nodes = [] %}
    {#
    Brute for loop through nodes to grab the one we want.
    This can be extended to support multiple sources, nodes, etc
    #}
    {% set source_nodes = graph.sources.values() if graph.sources else [] %}
    {% for node in source_nodes %}
        {% if node.source_name == external_source_name.strip() %}
            {{ log("Node acquired") }}
            {% do target_nodes.append(node) %}
        {% endif %}
    {% endfor %}

    {% if target_nodes | length == 0 %}
        {{ log("Missing target node - please check it matches existing source") }}
        {{ return(none) }}
    {% endif %}

    {{ log("Executing copy from stage") }}
    {%- set columns = target_nodes[0].columns.values() -%}
    {%- set external = target_nodes[0].external -%}
    {%- set is_csv = dbt_external_tables.is_csv(external.file_format) %}
    {%- set explicit_transaction = true -%}
    {%- set copy_into_target = source(target_nodes[0].source_name, target_nodes[0].name) -%}

    {{ log("Creating table if not exists") }}
    {%- call statement('create_table', fetch_result=True) %}
    create table if not exists {{ copy_into_target }}
     (
        value           variant,
        filename        varchar,
        rownumber       integer,
        _dbt_copied_at  timestamp
    );
    {% endcall %}

    {%- if explicit_transaction -%} begin; {%- endif %}

    {{ log("Executing copy from stage") }}
    {%- call statement('copy_execution', fetch_result=True) %}
        copy into {{ copy_into_target }}
        from (
            select
            {% if columns|length == 0 %}
                $1::variant as value,
            {% else -%}
            {%- for column in columns -%}
                {%- set col_expression -%}
                    {%- if is_csv -%}nullif($ {{ loop.index }},'') {# special case: get columns by ordinal position #}
                    {%- else -%}nullif($1:{{ column.name }},'')   {# standard behavior: get columns by name #}
                    {%- endif -%}
                {%- endset -%}
                {{ col_expression }}::{{ column.data_type }} as {{ column.name }},
            {% endfor -%}
            {% endif %}
                metadata$filename::varchar as metadata_filename,
                metadata$file_row_number::bigint as metadata_file_row_number,
                current_timestamp::timestamp as _dbt_copied_at
            from {{ external.location }} {# stage #}
        )
        file_format = {{ external.file_format }}
        {% if external.pattern -%} pattern = '{{ external.pattern }}' {%- endif %}
        ;
        {% if explicit_transaction -%} commit; {%- endif -%}
    {%- endcall -%}

    {{ log("Executed copy from stage. Exiting macro...") }}

{% endmacro %}
