
{# These macros create all the masking policies #}
{#
    To run:
    dbt run-operation create_all_masking_policies -t prd_pii

#}

{% macro create_all_masking_policies() %}

    {% set policies = ['string', 'float', 'date'] %}
    {% set masking_policy_db = var("common_masking_policy_db") %}
    {% set masking_policy_schema = var("common_masking_policy_schema") %}

    {% for policy in policies %}

        {% set call_masking_policy_macro = context["create_masking_policy_pii_full_field_" | string ~ policy | string] %}
        {% set result = run_query(call_masking_policy_macro(masking_policy_db, masking_policy_schema)) %}
        {{ print(result.columns[0].values()) }}

    {% endfor %}

    {% set sql %}

        SHOW MASKING POLICIES in {{ masking_policy_db }}.{{ masking_policy_schema }};

    {% endset %}

    {% set result = run_query(sql) %}
    {{ print(result.columns['name']) }}

{% endmacro %}
