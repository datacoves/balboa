
  create or replace  view BALBOA_STAGING.l2_covid_observations.dim_test
  
    
    
(
  
    "COUNTRY_REGION" COMMENT $$$$, 
  
    "PROVINCE_STATE" COMMENT $$$$, 
  
    "COUNTY" COMMENT $$$$, 
  
    "FIPS" COMMENT $$$$, 
  
    "DATE" COMMENT $$$$, 
  
    "CASE_TYPE" COMMENT $$$$, 
  
    "CASES" COMMENT $$$$, 
  
    "LONG" COMMENT $$$$, 
  
    "LAT" COMMENT $$$$, 
  
    "ISO3166_1" COMMENT $$$$, 
  
    "ISO3166_2" COMMENT $$$$, 
  
    "DIFFERENCE" COMMENT $$$$, 
  
    "LAST_UPDATED_DATE" COMMENT $$$$, 
  
    "LAST_REPORTED_FLAG" COMMENT $$$$, 
  
    "NEW_CASES" COMMENT $$$$
  
)

  copy grants as (
    select
    "COUNTRY_REGION",
  "PROVINCE_STATE",
  "COUNTY",
  "FIPS",
  "DATE",
  "CASE_TYPE",
  "CASES",
  "LONG",
  "LAT",
  "ISO3166_1",
  "ISO3166_2",
  "DIFFERENCE",
  "LAST_UPDATED_DATE",
  "LAST_REPORTED_FLAG",
  "NEW_CASES"
from l2_covid_observations.base_cases
  );
