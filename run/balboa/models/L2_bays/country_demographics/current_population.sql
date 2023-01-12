
  create or replace  view BALBOA_STAGING.l2_country_demographics.current_population
  
    
    
(
  
    "COUNTRY_CODE" COMMENT $$3 Letter Country Code$$, 
  
    "COUNTRY_NAME" COMMENT $$Name of the country$$, 
  
    "VALUE" COMMENT $$Total population for the country$$, 
  
    "YEAR" COMMENT $$Year population was collected$$
  
)

  copy grants as (
    with population_rank as (
    select
        country_code,
        country_name,
        value,
        year,
        rank() over (
            partition by country_code, country_name order by year desc
        ) as rank_years
    from l1_country_data._airbyte_raw_country_populations
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
  );
