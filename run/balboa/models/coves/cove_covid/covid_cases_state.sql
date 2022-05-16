

      create or replace transient table staging_BALBOA.cove_covid.covid_cases_state copy grants as
      (with covid_cases as (
    select * from bay_covid.covid_cases
),

location as (
    select * from bay_covid.covid_location
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
      );
    