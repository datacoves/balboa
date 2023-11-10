{# This macro creates schemas needed before creating masking policies #}
{#
    To run:
    dbt run-operation create_missing_schemas_with_masking_policy
#}

{%- macro create_missing_schemas_with_masking_policy() -%}

    {% set schemas = [] %}

    {% if var('use_common_masking_policy_db', 'False') == 'True' and target.name == 'prod' %}
        {% do schemas.append(var('common_masking_policy_db') ~ "." ~ var('common_masking_policy_schema')) %}
    {% else %}
        {# Get the schemas that have a meta tag #}
        {% for node in graph.nodes.values() -%}
            {% for column in node.columns -%}
                {% if node.columns[column]['meta'] | length > 0 %}
                    {% if var('use_common_masking_policy_schema', 'False') == 'True' %}
                        {% do schemas.append(node.database ~ "." ~ var('common_masking_policy_schema', node.schema)) %}
                    {% else %}
                        {% do schemas.append(node.database ~ "." ~ node.schema) %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {%- endfor -%}
    {% endif %}

    {% for schema in schemas|unique -%}
        {% set db_name = schema.split(".")[0] %}
        {% set schema_name = schema.split(".")[1] %}

        {% do create_custom_schema(db_name, schema_name) %}
    {%- endfor -%}

    {# TODO improve this so we only create when it is a new schema
    {% do snow_mask_reapply_policies('unapply') %}
    {% do dbt_snow_mask.create_masking_policy(resource_type='sources') %}
    {% do dbt_snow_mask.create_masking_policy(resource_type='models') %}#}

{%- endmacro -%}
