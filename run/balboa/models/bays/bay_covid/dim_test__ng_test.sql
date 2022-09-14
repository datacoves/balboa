
  create or replace  view BALBOA_STAGING.bay_covid.dim_test
  
    
    
(
  
    
      CASES
    
    , 
  
    
      DEATHS
    
    , 
  
    
      DATE
    
    , 
  
    
      FIPS
    
    , 
  
    
      STATE
    
    , 
  
    
      _AIRBYTE_AB_ID
    
    , 
  
    
      _AIRBYTE_EMITTED_AT
    
    
  
)

  copy grants as (
    select
    "CASES",
  "DEATHS",
  "DATE",
  "FIPS",
  "STATE",
  "_AIRBYTE_AB_ID",
  "_AIRBYTE_EMITTED_AT"
from BALBOA_STAGING.bay_covid.base_cases
  );
