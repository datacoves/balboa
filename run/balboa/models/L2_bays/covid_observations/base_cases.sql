
  create or replace  view BALBOA_STAGING.l2_covid_observations.base_cases
  
    
    
(
  
    "COUNTRY_REGION" COMMENT $$$$, 
  
    "PROVINCE_STATE" COMMENT $$$$, 
  
    "COUNTY" COMMENT $$$$, 
  
    "FIPS" COMMENT $$FIPS Code$$, 
  
    "DATE" COMMENT $$Reporting date$$, 
  
    "CASE_TYPE" COMMENT $$$$, 
  
    "CASES" COMMENT $$Reported Covid-19 cases$$, 
  
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
    with final as (

    select
        *,
        difference as new_cases
    from l1_starschema_covid19.jhu_covid_19

)

select * from final
  );
