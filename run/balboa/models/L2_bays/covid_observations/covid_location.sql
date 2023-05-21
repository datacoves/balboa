
  create or replace  view BALBOA_STAGING.l2_covid_observations.covid_location
  
    
    
(
  
    "LOCATION_ID" COMMENT $$$$, 
  
    "COUNTRY" COMMENT $$$$, 
  
    "STATE" COMMENT $$$$, 
  
    "COUNTY" COMMENT $$$$, 
  
    "LAT" COMMENT $$$$, 
  
    "LONG" COMMENT $$$$, 
  
    "ISO3166_1" COMMENT $$$$, 
  
    "ISO3166_2" COMMENT $$$$
  
)

  copy grants as (
    

with jhu_covid_19 as (
    select distinct
        country_region,
        coalesce(province_state, 'UNDEFINED') as province_state,
        coalesce(county, 'UNDEFINED') as county,
        lat,
        long,
        iso3166_1,
        iso3166_2,
        date
    from l1_starschema_covid19.jhu_covid_19
),

rank_locations as (
    select
        hash(country_region || '|' || province_state || '|' || county) as snowflake_location_id,
        md5(cast(coalesce(cast(country_region as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(province_state as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(county as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as location_id,
        country_region as country,
        province_state as state,
        county,
        lat,
        long,
        iso3166_1,
        iso3166_2,
        rank() over (partition by location_id order by date desc) as rowrank
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
  );
