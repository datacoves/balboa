with raw_cases as (
    select
        country_region,
        province_state,
        county,
        cases,
        date,
        case_type
    from {{ source('starschema_covid19', 'jhu_covid_19') }}
),

create_location_id as (
    select
        {{ dbt_utils.surrogate_key(['country_region', 'province_state', 'county']) }} as location_id,
        cases,
        date,
        case_type
    from raw_cases
),

pivoted_model as (
    select
        location_id,
        date,
        sum("'Confirmed'") as confirmed,
        sum("'Deaths'") as deaths,
        sum("'Active'") as active,
        sum("'Recovered'") as recovered
    from create_location_id
    pivot (sum(cases) for case_type in( 'Confirmed', 'Deaths', 'Active', 'Recovered' )) as case_pivot
    group by location_id, date
)

select *
from pivoted_model
