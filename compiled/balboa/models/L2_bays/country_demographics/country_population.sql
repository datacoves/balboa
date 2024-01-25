with population_rank as (
    select
        country_code,
        country_name,
        value,
        year,
        rank() over (
            partition by country_code, country_name order by year desc
        ) as rank_years
    from BALBOA.l1_country_data.country_populations
)

select
    country_code,
    country_name,
    value,
    year
from population_rank
where
    rank_years = 1
    and year > 2017