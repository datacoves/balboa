

with jhu_covid_19 as (
    select
        country_region,
        province_state,
        county,
        lat,
        long,
        iso3166_1,
        iso3166_2,
        date
    from BALBOA.source_starschema_covid19.jhu_covid_19
),

rank_locations as (
    select
        hash(country_region || '|' || province_state || '|' || county) as snowflake_location_id,
        md5(cast(coalesce(cast(country_region as 
    varchar
), '') || '-' || coalesce(cast(province_state as 
    varchar
), '') || '-' || coalesce(cast(county as 
    varchar
), '') as 
    varchar
)) as location_id,
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