{#  This test validates that all declared keys are found in a json variant.
    Usage:

  sources:
    - name: source_name
        tables:
          - name: raw_account
            columns:
              - name: _airbyte_data
                tests:
                  # This test assures that columns exist in the JSON blob in the _airbyte_data column
                  - expect_keys_to_exist_in_json:
                      value_set: ['id', 'name', 'date', 'status']
                      rows_to_check: 3
#}

{% test expect_keys_to_exist_in_json(model, column_name, value_set, rows_to_check=1) %}

with all_values as (
    select distinct trim(value,'"') as value_field
    from (
        select *
        from {{ model }}
        {% if row_condition %}
            where {{ row_condition }}
        {% endif %}
        limit {{ rows_to_check }}
    ) source,
        lateral flatten(object_keys({{column_name}}))
),

set_values as (

    {% for value in value_set -%}
    select
        '{{ value }}' as value_field
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

validation_errors as (
    -- values from the set that are not in the json
    select
        s.value_field
    from
        set_values s
        left join
        all_values v on v.value_field = s.value_field
    where
        v.value_field is null
)

select *
from validation_errors

{% endtest %}