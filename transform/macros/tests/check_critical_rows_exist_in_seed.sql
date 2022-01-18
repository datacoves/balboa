{#  This test validates that all rows in a seed are found with values intact in a specific model.
    The primary code source is the dbt_utils equality test.
    Usage:
    model_name:
        tests:
        - check_critical_rows_exist_in_seed:
            compare_seed: ref('other_table_name')
            compare_columns: {# Optional #}
                - first_column
                - second_column
     #}
{% test check_critical_rows_exist_in_seed(model, compare_seed, compare_columns=None) %}

{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}

-- setup


{#
{%- do dbt_utils._is_relation(model, 'check_critical_rows_exist_in_seed') -%}
This was in the dbt_utils model I modeled this one after, but doesn't run here.
#}

{#-
If the compare_cols arg is provided, we can run this test without querying the
information schema — this allows the model to be an ephemeral model
-#}

{%- if not compare_columns -%}
    {%- do dbt_utils._is_ephemeral(model, 'check_critical_rows_exist_in_seed') -%}
    {%- set compare_columns = adapter.get_columns_in_relation(model) | map(attribute='quoted') -%}
{%- endif -%}

{% set compare_cols_csv = compare_columns | join(', ') %}

with a as (

    select * from {{ model }}

),

b as (

    select * from {{ compare_seed }}

),

b_minus_a as (

    select {{compare_cols_csv}} from b
    {{ dbt_utils.except() }}
    select {{compare_cols_csv}} from a

)

select * from b_minus_a

{% endtest %}