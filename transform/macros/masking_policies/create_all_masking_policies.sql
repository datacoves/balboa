{# These macros create all the masking policies #}
{#

    To run:
    dbt run-operation create_all_masking_policies -t prd_pii

#}

{% macro create_all_masking_policies() %}

    {# Defines the policy types and the return value #}
    {% set policies = [
        ('string' , "********"),
        ('float' , 0.00),
        ('number' , 0),
        ('date' , '0001-01-01 00:00:00.000' ),
        ('timestamp_tz', '0001-01-01 00:00:00.000 +0000' ),
        ('variant', ("masked","data"))
    ] %}

    {% set masking_policy_db = var("common_masking_policy_db") %}
    {% set masking_policy_schema = var("common_masking_policy_schema") %}

    {% set existing_policies = ( masking_policy_list() ) %}

    {% for data_type, mask_value in policies %}

        {% set policy_name = ("masking_policy_pii_" ~ data_type | string) | upper %}
        {% set fully_qualified_policy_name = masking_policy_db + '.' + masking_policy_schema + '.' + policy_name %}

        {% if policy_name in existing_policies%}
            {% set policy_currently_in_use = masking_policy_in_use(fully_qualified_policy_name) %}
        {% else %}
            {% set policy_currently_in_use = false %}
        {% endif %}

        {% set result = run_query(
                create_update_masking_policy_pii(
                        masking_policy_db,
                        masking_policy_schema,
                        data_type,
                        mask_value,
                        policy_currently_in_use
                        )
                    ) %}

    {% endfor %}

    {{ print("The following masking policies were created / updated: ")}}
    {{ print( masking_policy_list() ) }}
{% endmacro %}
