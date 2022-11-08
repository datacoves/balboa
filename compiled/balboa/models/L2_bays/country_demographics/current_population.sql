select
    country_code,
    country_name,
    value,
    year
from (
        select
            country_code,
            country_name,
            value,
            year,
            rank() over (
                partition by country_code, country_name order by year desc
            ) as rank_years
        from BALBOA.l1_country_data._airbyte_raw_country_populations
    )
where
    rank_years = 1
    and year > 2017