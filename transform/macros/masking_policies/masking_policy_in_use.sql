{# These macros create all the masking policies #}
{#
    To run:
    dbt run-operation masking_policy_in_use --args '{"policy_name": "BALBOA.MASKING_POLICIES.masking_policy_pii_full_field_float"}' -t prd_pii

#}

{% macro masking_policy_in_use(policy_name) %}

    {% set sql %}
        select POLICY_NAME
        from table( information_schema.policy_references( policy_name => '{{policy_name}}' ))
        group by 1;
    {% endset %}

    {% set result = run_query(sql) %}

    {% if result %}
        {{ return(true) }}
    {% else %}
        {{ return(false) }}
    {% endif %}

{% endmacro %}
