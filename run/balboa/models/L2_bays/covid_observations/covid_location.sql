
  create or replace   view BALBOA_STAGING.L2_COVID_OBSERVATIONS.covid_location
  
    
    
(
  
    "LOCATION_ID" COMMENT $$Unique identifier for the location$$, 
  
    "COUNTRY" COMMENT $$Name of the country$$, 
  
    "STATE" COMMENT $$Name of the state$$, 
  
    "COUNTY" COMMENT $$Name of the county$$, 
  
    "LAT" COMMENT $$Latitude coordinate$$, 
  
    "LONG" COMMENT $$Longitude coordinate$$, 
  
    "ISO3166_1" COMMENT $$ISO 3166-1 code for the country$$, 
  
    "ISO3166_2" COMMENT $$ISO 3166-2 code for the country$$
  
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
    from L1_COVID19_EPIDEMIOLOGICAL_DATA.jhu_covid_19
),

rank_locations as (
    select
        hash(
            country_region || '|' || province_state || '|' || county
        ) as snowflake_location_id,
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

