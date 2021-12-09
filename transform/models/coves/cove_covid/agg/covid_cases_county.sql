with covid_cases as (
    select * from {{ ref('int_covid_cases') }}
),

location as (
    select * from {{ ref('location') }}
)

select
    location.country,
    location.state,
    location.county,
    location.lat,
    location.long,
    cases.date,
    cases.confirmed,
    cases.deaths,
    cases.active,
    cases.recovered
from covid_cases as cases
left join location as location
    on location.location_id = cases.location_id
where location.county is not null
