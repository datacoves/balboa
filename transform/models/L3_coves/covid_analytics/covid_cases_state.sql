with covid_cases as (
    select * from {{ ref('total_covid_cases') }}
),

location as (
    select * from {{ ref('covid_location') }}
)

select
    location.location_id,
    location.country,
    location.state,
    location.lat,
    location.long,
    covid_cases.date,
    covid_cases.confirmed as cases,
    covid_cases.deaths,
    covid_cases.active,
    covid_cases.recovered
from covid_cases
inner join location
    on location.location_id = covid_cases.location_id
where
    location.state is not null
    and location.county is not null
