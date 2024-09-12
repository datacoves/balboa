with population_rank as (
    select
        country_code,
        country_name,
        value,
        year,
        rank() over (
            partition by country_code, country_name order by year desc
        ) as rank_years
    from BALBOA.L1_COUNTRY_DATA.country_populations

)

select
    country_code,
    country_name,
    value as total_population,
    year
from population_rank
where
    rank_years = 1
    and year > 2017