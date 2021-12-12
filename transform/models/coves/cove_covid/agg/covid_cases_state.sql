with covid_cases as (
    select * from {{ ref('covid_cases') }}
),

location as (
    select * from {{ ref('location') }}
)

select
    location.country,
    location.state,
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
where location.state is not null
    and location.county is null
