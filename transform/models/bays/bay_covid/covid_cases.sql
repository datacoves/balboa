with cases as (
    select
        hash(country || '|' || state || '|' || county) as location_id,
        cases,
        date,
        case_type
    from {{ source('jhu_covid_19', 'jhu_covid_19') }}
),
pivoted_model as (
    select
        location_id,
        date,
        {{ dbt_utils.pivot('case_type', dbt_utils.get_column_values(cases, 'case_type')), then_value='cases' }}
    from cases
    group by location_id, date
)
