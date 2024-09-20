
  create or replace   view BALBOA_STAGING.L2_COUNTRY_DEMOGRAPHICS.country_population
  
    
    
(
  
    "COUNTRY_CODE" COMMENT $$3 Letter Country Code$$, 
  
    "COUNTRY_NAME" COMMENT $$Name of the country$$, 
  
    "TOTAL_POPULATION" COMMENT $$Total population for the country$$, 
  
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
    from L1_COUNTRY_DATA.country_populations_v1
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
  );

