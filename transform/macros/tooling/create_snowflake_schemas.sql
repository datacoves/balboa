{# Macro for creating a list of Schemas in Snowflake. #}
{# 
    Run using
    dbt run-operation create_snowflake_schemas  --args 'objects_to_be_created'
#}

{% macro create_snowflake_schemas(objects_to_be_created) %}
    {% for schema in objects_to_be_created %}        
            {% set schema_as_list = schema.split('.') %}
            {% set db = schema_as_list[0] %}
            {% set schema = schema_as_list[1] %}
            {% set create_schema_sql %}
                use role transformer_dbt_prd;
                use database {{db}};
                create schema {{schema}};
            {% endset %}

            {% do run_query(create_schema_sql) %}
            {{ log("Schema "~db~'.'~schema~ " created" , info=true) }}
        
    {% endfor %}
{% endmacro %}