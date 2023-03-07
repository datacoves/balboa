
  
    

        create or replace transient table BALBOA_STAGING.l3_covid_analytics.covid_cases_state copy grants as
        (with  __dbt__cte__covid_location as (


with jhu_covid_19 as (
    select distinct
        country_region,
        COALESCE(province_state, 'UNDEFINED') as province_state,
        COALESCE(county, 'UNDEFINED') as county,
        lat,
        long,
        iso3166_1,
        iso3166_2,
        date
    from l1_starschema_covid19.jhu_covid_19
),

rank_locations as (
    select
        HASH(country_region || '|' || province_state || '|' || county) as snowflake_location_id,
        
    
md5(cast(coalesce(cast(country_region as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(province_state as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(county as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id, --noqa
        country_region as country,
        province_state as state,
        county,
        lat,
        long,
        iso3166_1,
        iso3166_2,
        RANK() over (partition by location_id order by date desc) as rowrank
    from jhu_covid_19
)

select
    location_id,
    country,
    state,
    county,
    lat,
    long,
    iso3166_1,
    iso3166_2
from rank_locations
where rowrank = 1
),covid_cases as (
    select * from l2_covid_observations.total_covid_cases
),

location as (
    select * from __dbt__cte__covid_location
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
left join location
    on location.location_id = covid_cases.location_id
where location.state is not null
    and location.county is not null
        );
      
  