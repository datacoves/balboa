with _airbyte_raw_country_populations as (
    select * from {{ ref('_airbyte_raw_country_populations') }}
),

ranked_pop as (
    select
        country_code,
        country_name,
        value,
        year,
        rank() over (
            partition by country_code, country_name order by year desc
        ) as rank_years
    from _airbyte_raw_country_populations
),

estimate_current_pop as (
    select
        ranked_pop.value as most_recent_population,
        five_periods_earlier_pop.value as five_years_earlier_population,
        ranked_pop.country_code,
        ranked_pop.country_name,
        ranked_pop.year as most_recent_population_year,
        (ranked_pop.value - five_periods_earlier_pop.value) as population_gain_last_five_periods,
        (ranked_pop.year - five_periods_earlier_pop.year) as years_between_last_five_periods,
        year(current_timestamp()) - ranked_pop.year as years_since_last_pop,
        population_gain_last_five_periods / years_between_last_five_periods
        * years_since_last_pop + ranked_pop.value as estimate_current_population
    from ranked_pop
    inner join ranked_pop as five_periods_earlier_pop
        on
            five_periods_earlier_pop.country_code = ranked_pop.country_code
            and five_periods_earlier_pop.rank_years = 5
    where
        ranked_pop.rank_years = 1
)

select
    country_code,
    most_recent_population,
    most_recent_population_year,
    estimate_current_population
from estimate_current_pop
