{# Macro for creating a list of Roles in Snowflake. #}
{#
    Run using
    dbt run-operation create_snowflake_schemas  --args 'schemas_to_be_created'
#}

{% macro create_snowflake_schemas(schemas_to_be_created) %}
    {% for schema in schemas_to_be_created %}

        {% set create_schema_sql %}
            use role transformer_dbt;
            create schema {{ schema.upper() }};
        {% endset %}

        {% do run_query(create_schema_sql) %}
        {{ log("Schema " ~ schema ~ " created", info=true) }}

    {% endfor %}
{% endmacro %}
