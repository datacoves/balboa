{# This macro reapplies masking policies #}
{#
    To run:
    dbt run-operation snow_mask_reapply_policies
#}

{% macro snow_mask_reapply_policies(operation_type="apply") %}

    {% set schema_list = [] %}
    {% set schemas =  list_schemas() %}
    {% for schema in schemas %}
        {% do schema_list.append(schema['name']) %}
    {% endfor %}

    {% for node in graph.nodes.values() -%}

        {% set database = node.database | string %}
        {% set schema   = node.schema | string | upper %}
        {% set masking_policy_database = database %}
        {% set masking_policy_schema = schema %}
        {# Check if common masking policy database is set #}
        {% if var('use_common_masking_policy_db', 'False') %}
            {% set masking_policy_database = var('common_masking_policy_db', database) %}
            {% set masking_policy_schema = var('common_masking_policy_schema', schema) %}
        {% endif %}
        {# Check if common masking policy schema is set #}
        {% if var('use_common_masking_policy_schema', 'False') %}
            {% set masking_policy_schema = var('common_masking_policy_schema', schema) %}
        {% endif %}

        {% set name   = node.name | string %}
        {% set identifier = (node.identifier | default(name, True)) | string %}

        {% set unique_id = node.unique_id | string %}
        {% set resource_type = node.resource_type | string %}
        {% set materialization = node.config.materialized | string %}

        {% set meta_key = 'masking_policy' %}

        {# Only run if there is a schema in the db for this relation #}
        {% set schema_exists = schema in schema_list %}

        {# If other materializations are added that create views or tables, #}
        {# this list will need to update #}
        {% if (schema_exists) and (materialization in ['table', 'view', 'incremental']) %}

            {# override materialization_type for incremental to table #}
            {% if materialization == 'incremental' %}
                {% set materialization = 'table' %}
            {% endif %}

            {% set meta_columns = dbt_snow_mask.get_meta_objects(unique_id, meta_key, resource_type) %}

            {% set masking_policy_list_sql %}
                show masking policies in {{masking_policy_database}}.{{masking_policy_schema}};
                select $3||'.'||$4||'.'||$2 as masking_policy from table(result_scan(last_query_id()));
            {% endset %}

            {%- for meta_tuple in meta_columns if meta_columns | length > 0 %}
                {% set column   = meta_tuple[0] %}
                {% set masking_policy_name  = meta_tuple[1] %}

                {% if masking_policy_name is not none %}

                    {% set masking_policy_list = dbt_utils.get_query_results_as_dict(masking_policy_list_sql) %}

                    {% set timestamp = modules.datetime.datetime.now().strftime("%H:%M:%S") ~ " | " %}

                    {% set policies_in_database = masking_policy_list['MASKING_POLICY'] | list %}
                    {% set full_masking_policy_name = masking_policy_database|upper ~ '.' ~ masking_policy_schema|upper ~ '.' ~ masking_policy_name|upper %}
                    {% if (operation_type == 'apply') and (full_masking_policy_name not in policies_in_database) %}
                        {{ log(timestamp ~ "Missing Masking policy defined for "~ unique_id ~" column: " ~ column,true) }}
                        {{ log(timestamp ~ "Policy Name: " ~ full_masking_policy_name, true) }}
                        {# Force an exit when masking policy is missing #}
                        {{ 0/0 }}
                    {% endif %}

                    {% for masking_policy_in_db in masking_policy_list['MASKING_POLICY'] %}

                        {%- set relation = adapter.get_relation(
                            database=database,
                            schema=schema,
                            identifier=identifier) -%}

                        {% set relation_exists = relation is not none  %}

                        {% if (relation_exists) and (full_masking_policy_name == masking_policy_in_db) %}
                            {{ log(timestamp ~ operation_type ~ "ing masking policy to model : " ~ full_masking_policy_name ~ " on " ~ database ~ '.' ~ schema ~ '.' ~ identifier ~ '.' ~ column, info=True) }}
                            {% set query %}
                                {% if operation_type == "apply" %}
                                    alter {{materialization}}  {{database}}.{{schema}}.{{identifier}} modify column  {{column}} set masking policy  {{masking_policy_database}}.{{masking_policy_schema}}.{{masking_policy_name}}
                                {% elif operation_type == "unapply" %}
                                    alter {{materialization}}  {{database}}.{{schema}}.{{identifier}} modify column  {{column}} unset masking policy
                                {% endif %}
                            {% endset %}

                            {% do run_query(query) %}
                        {% elif (full_masking_policy_name == masking_policy_in_db) %}
                            {{ log(timestamp ~ "Skipping non-existant relation " ~ database ~ '.' ~ schema ~ '.' ~ identifier ~ ' column: ' ~ column, info=True) }}
                        {% endif %}
                    {% endfor %}
                {% endif %}

            {% endfor %} #}
        {% endif%}

    {% endfor %}

{% endmacro %}
