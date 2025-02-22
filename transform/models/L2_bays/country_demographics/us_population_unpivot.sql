{# exploration using example from
    https://github.com/dataprofessor/population-dashboard/blob/master/US_Population.ipynb
 #}

with us_population as (
    select *
    from {{ ref("stg_us_population") }}
    unpivot (
        population for
        year in ("2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019")
    )
),

states as (
    select * from {{ ref("state_codes") }}
),

population_info as (
    select
        us_population.state_id,
        us_population.state_name,
        states.state_code,
        us_population.year,
        us_population.population,
        LAG(
            us_population.population, 1, 0) over (
            partition by us_population.state_id, us_population.state_name
            order by us_population.year
        ) as previous_population
    from us_population
    join states
        on us_population.state_name = states.state_name
),

final as (
    select
        *,
        population - previous_population as population_difference,
        ABS(population_difference) as absolute_population_difference
    from population_info
    where year = 2019
)

select * from final
order by year asc, population desc
