
  
    

        create or replace transient table BALBOA_STAGING.l3_covid_analytics.covid_cases_country copy grants as
        (with covid_cases as (
    select
        location_id,
        date,
        confirmed,
        deaths,
        active,
        recovered
    from l2_covid_observations.total_covid_cases
),

location as (
    select
        location_id,
        state,
        country,
        lat,
        long
    from l2_covid_observations.covid_location
)

select
    location.country,
    location.lat,
    location.long,
    covid_cases.date,
    covid_cases.confirmed,
    covid_cases.deaths,
    covid_cases.active,
    covid_cases.recovered
from covid_cases
join location
    on location.location_id = covid_cases.location_id
where
    location.country is not null
    and location.state is null
        );
      
  