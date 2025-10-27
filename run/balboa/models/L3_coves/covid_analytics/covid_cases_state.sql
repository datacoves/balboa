
  
    

create or replace transient table BALBOA_STAGING.L3_COVID_ANALYTICS.covid_cases_state
    

    copy grants
    as (with covid_cases as (
    select * from L2_COVID_OBSERVATIONS.total_covid_cases
),

location as (
    select * from L2_COVID_OBSERVATIONS.covid_location
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
    on covid_cases.location_id = location.location_id
where
    location.state is not null
    and location.county is not null
    )
;


  