  select
    country_code,
    country_name,
    value as population,
    year
  from (
    select
    country_code
    , country_name
    , value
    , year
    , rank() over (partition by country_code, country_name order by year desc) as rank_years
  from {{ ref('_airbyte_raw_country_populations') }}
  )
  where
    rank_years = 1