{# Macro for creating a list of Databases in Snowflake. #}
{# 
    Run using
    dbt run-operation create_snowflake_databases  --args 'objects_to_be_created'
#}

{% macro create_snowflake_databases(objects_to_be_created) %}
    {% for database in objects_to_be_created %}        
        
            {% set create_database_sql %}
                use role transformer_dbt_prd;
                create database {{database.upper()}};
            {% endset %}

            {% do run_query(create_database_sql) %}
            {{ log("Database "~database~" created", info=true) }}
        
    {% endfor %}
{% endmacro %}
