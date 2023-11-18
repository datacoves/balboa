
{# These macros create all the masking policies #}
{#
    To run:
    dbt run-operation masking_policy_list -t prd_pii

#}

{% macro masking_policy_list() %}

    {% set masking_policy_db = var("common_masking_policy_db") %}
    {% set masking_policy_schema = var("common_masking_policy_schema") %}

    {% set sql %}
        SHOW MASKING POLICIES in {{ masking_policy_db }}.{{ masking_policy_schema }};
    {% endset %}

    {% set result = run_query(sql) %}
    {{ return( result.columns['name'].values() | list ) }}

{% endmacro %}
