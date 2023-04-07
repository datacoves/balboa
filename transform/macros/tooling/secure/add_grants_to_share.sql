{# Macro used to re-grant access to a share after a dbt run. #}
{#
    Run using
    dbt run-operation add_grants_to_share  --args 'my_share'
#}

{% macro add_grants_to_share(share_name) %}

    {% if target.name == 'prd' %}

        {% set grant_sql %}
            use role securityadmin;
            grant select on {{ this }} to share {{ share_name }}
        {% endset %}

        {% do run_query(grant_sql) %}

    {% endif %}

{% endmacro %}
