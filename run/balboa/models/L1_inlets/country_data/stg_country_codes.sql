
  create or replace   view BALBOA_STAGING.L1_COUNTRY_DATA.stg_country_codes
  
    
    
(
  
    "COUNTRY_NAME" COMMENT $$Name of the country$$, 
  
    "ISO_2" COMMENT $$Two-letter ISO country code$$, 
  
    "ISO_3" COMMENT $$Three-letter ISO country code$$, 
  
    "NUMERIC_CODE" COMMENT $$Numeric country code$$
  
)

  copy grants
  
  
  as (
    select
    country_name,
    alpha_2_code as iso_2,
    alpha_3_code as iso_3,
    numeric_code
from SEEDS.country_codes
  );

