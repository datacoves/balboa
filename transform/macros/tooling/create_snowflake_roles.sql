{# Macro for creating a list of Roles in Snowflake. #}
{# 
    Run using
    dbt run-operation create_snowflake_roles  --args 'objects_to_be_created'
#}

{% macro create_snowflake_roles(objects_to_be_created) %}
    {% for role in objects_to_be_created %}        
        
            {% set create_role_sql %}
                use role securityadmin;
                create role {{role.upper()}};
            {% endset %}

            {% do run_query(create_role_sql) %}
            {{ log("Role "~role~" created", info=true) }}
        
    {% endfor %}
{% endmacro %}
