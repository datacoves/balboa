{# Macro for creating a list of Roles in Snowflake. #}
{# 
    Run using
    dbt run-operation create_snowflake_roles  --args 'roles_to_be_created'
#}

{% macro create_snowflake_roles(roles_to_be_created) %}
    {% for role in roles_to_be_created %}        
        
        {% set create_role_sql %}
            use role securityadmin;
            create role {{ role.upper() }};
        {% endset %}

        {% do run_query(create_role_sql) %}
        {{ print("Role "~role~" created") }}
    
    {% endfor %}
{% endmacro %}
