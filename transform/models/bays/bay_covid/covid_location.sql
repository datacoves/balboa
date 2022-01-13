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
    from {{ ref('jhu_covid_19') }}
),

rank_locations as (
    select
        hash(country_region || '|' || province_state || '|' || county) as snowflake_location_id,
        {{ dbt_utils.surrogate_key(['country_region', 'province_state', 'county']) }} as location_id,
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
