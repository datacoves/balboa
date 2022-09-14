
  create or replace  view BALBOA_STAGING.bay_country.current_population
  
    
    
(
  
    
      COUNTRY_CODE
    
    , 
  
    
      COUNTRY_NAME
    
    , 
  
    
      VALUE
    
    , 
  
    
      YEAR
    
    
  
)

  copy grants as (
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
        from BALBOA_STAGING.inlet_country_data._airbyte_raw_country_populations
    )
where
    rank_years = 1
    and year > 2017
  );
