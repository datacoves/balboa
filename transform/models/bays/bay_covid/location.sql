with jhu_covid_19 as (
    select
        country,
        state,
        county,
        lat,
        long,
        iso3166_1,
        iso3166_2
    from {{ source('jhu_covid_19', 'jhu_covid_19') }}
)
select
    hash(country || '|' || state || '|' || county) as location_id,
    lat,
    long,
    iso3166_1,
    iso3166_2
from jhu_covid_19