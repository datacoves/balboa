{# Macro for checking a database does not exist in Snowflake. #}
{# Will return error on stderr if db exists. #}
{# For use in blue-green runs. #}
{# 
    Run using
    dbt run-operation check_db_exists --args 'db_name: demo_db'
#}

{% macro check_db_does_not_exist(db_name) %}

    {% set results = run_query("show databases like '" ~ db_name ~ "'") %}
    {% if results %}
        {{ exceptions.raise_compiler_error("Database exists.") }}
    {% endif %}

{% endmacro %}
