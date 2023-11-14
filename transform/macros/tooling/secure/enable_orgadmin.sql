{# Macro for setting up Snowflake objects. #}
{#PWFIRFI.UGA35306
    Run using
    dbt run-operation enable_orgadmin  --args {'run_from_account: UGA35306, enable_on_account: main_pii'} -t prd
#}

{%- macro enable_orgadmin(run_from_account, enable_on_account) -%}

        {% if run_from_account == enable_on_account %}
            use role accountadmin;
        {% else %}
            use role orgadmin;
        {% endif %}
        alter account {{account}} set is_org_admin = true;

{%- endmacro -%}
