select
    country_code,
    country_name,
    value,
    year,
    1 as sql_col
from (
        select
            country_code,
            country_name,
            value,
            year,
            rank() over (
                partition by country_code, country_name order by year desc
            ) as rank_years
        from {{ ref('_airbyte_raw_country_populations') }}
    )
where
    rank_years = 1
    and year > 2017
