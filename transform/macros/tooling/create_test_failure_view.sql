{# macros/create_test_failure_view.sql #}
{#
  This macro creates a view that aggregates the results of all non-table tests in the project, for use in live testing
  of views and dynamic tables.
  Before running this macro, create a model with the below content named test_failures.sql anywhere in your models folder.
  It only needs to be run once, but any additional runs will not cause issues as on-run-end will always replace it.
    {{ config( materialized='view') }}
    select
        'no_tests' as test_name,
        0 as count_failed,
        null as error_state,
        null as warning_state,
        parse_json('[]') as tested_models
    where 1 = 0
#}

{# dbt must be wirh with --vars '{"persist_tests": "true", "tests_model": "test_failures"}' #}

{% macro create_test_failure_view(results) -%}
{% if execute and var('persist_tests', 'false') == 'true' %}
    {% set test_failures_unique_id = "model." ~ project_name ~ ".stg_test_failures" %}
    {% set test_selects = [] %}
    {% set test_failures_node = graph.nodes[test_failures_unique_id] %}
    {% set all_test_failures_location = (
        test_failures_node.database ~ '.' ~
        test_failures_node.schema ~ '.' ~
        test_failures_node.alias
    ) %}

    {% for res in results -%}
        {% if res.node.resource_type == 'test' -%}
            {% set test_name = res.node.name %}
            {% set test_unique_id = res.node.unique_id %}
            {% set compiled_sql = res.node.compiled_code | replace(';', '') %}

            {% set error_if_condition = res.node.config.error_if if res.node.config.error_if is defined else "!= 0" %}
            {% set warn_if_condition = res.node.config.warn_if if res.node.config.warn_if is defined else "!= 0" %}

            {% set test_node = graph.nodes[test_unique_id] %}
            {% set dep_nodes = test_node.depends_on.nodes if test_node.depends_on is defined else [] %}

            {% set tested_model_nodes = [] %}
            {% for dep_node_id in dep_nodes -%}
                {% set dep_node = graph.nodes.get(dep_node_id) %}
                {% if dep_node is not none and dep_node.resource_type == 'model' -%}
                    {% do tested_model_nodes.append(dep_node) %}
                {%- endif %}
            {%- endfor %}

            {% set ns = namespace(all_skippable_tables=true) %}
            {% for m in tested_model_nodes -%}
                {% if m.config.materialized not in ["table"] -%}
                    {% set ns.all_skippable_tables = false %}
                {%- endif %}
            {%- endfor %}

            {% if ns.all_skippable_tables == true -%}
                {% continue %}
            {%- endif %}

            {% set tested_model_aliases = [] %}
            {% for m in tested_model_nodes -%}
                {% do tested_model_aliases.append(m.alias) %}
            {%- endfor %}

            {% set tested_models_json = 'parse_json(\'' ~ tested_model_aliases | tojson ~ '\')' %}

            {% set test_select %}
            select
                '{{ test_name }}' as test_name,
                (select count(*) from ({{ compiled_sql | trim }})) as count_failed,
                case
                    when (select count(*) from ({{ compiled_sql | trim }})) {{ error_if_condition }}
                    then 'ERROR'
                    else null
                end as error_state,
                case
                    when (select count(*) from ({{ compiled_sql | trim }})) {{ warn_if_condition }}
                    then 'WARNING'
                    else null
                end as warning_state,
                {{ tested_models_json }} as tested_models
            {% endset %}

            {% do test_selects.append(test_select) %}
        {%- endif %}
    {%- endfor %}

    {% if test_selects|length == 0 -%}
        {% set final_sql = "
            select
                'no_tests' as test_name,
                0 as count_failed,
                null as error_state,
                null as warning_state,
                parse_json('[]') as tested_models
            where 1=0"
        %}
    {% else -%}
        {% set final_sql = test_selects | join(' union all ') %}
    {%- endif %}

    {% do run_query("create or replace view " ~ all_test_failures_location ~ " as " ~ final_sql) %}
{%- endif %}
{%- endmacro %}
