
  create or replace   view BALBOA_STAGING.L2_COUNTRY_DEMOGRAPHICS.us_population_unpivot
  
    
    
(
  
    "ID" COMMENT $$The unique identifier of the population record.$$, 
  
    "STATE_NAME" COMMENT $$The name of the state where the population record is associated with.$$, 
  
    "STATE_CODE" COMMENT $$The code representing the state where the population record is associated with.$$, 
  
    "YEAR" COMMENT $$The year of the population record.$$, 
  
    "POPULATION" COMMENT $$The population count for a specific state and year.$$, 
  
    "PREVIOUS_POPULATION" COMMENT $$The population count for the previous year.$$, 
  
    "POPULATION_DIFFERENCE" COMMENT $$The difference in population between the current year and the previous year.$$, 
  
    "ABSOLUTE_POPULATION_DIFFERENCE" COMMENT $$The absolute value of the population difference, ignoring the positive or negative sign.$$
  
)

  copy grants as (
    

with us_population as (
    select *
    from L1_COUNTRY_DATA.us_population
    unpivot (
        population for
        year in ("2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019")
    )
),

states as (
    select * from SEEDS.state_codes
),

population_info as (
    select
        us_population.id,
        us_population.state_name,
        states.state_code,
        us_population.year,
        us_population.population,
        LAG(
            us_population.population, 1, 0) over (
            partition by us_population.id, us_population.state_name
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
  );

