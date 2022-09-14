
  create or replace  view BALBOA_STAGING.inlet_starschema_covid19.jhu_covid_19
  
    
    
(
  
    
      COUNTRY_REGION
    
    , 
  
    
      PROVINCE_STATE
    
    , 
  
    
      COUNTY
    
    , 
  
    
      FIPS
    
    , 
  
    
      DATE
    
    , 
  
    
      CASE_TYPE
    
    , 
  
    
      CASES
    
    , 
  
    
      LONG
    
    , 
  
    
      LAT
    
    , 
  
    
      ISO3166_1
    
    , 
  
    
      ISO3166_2
    
    , 
  
    
      DIFFERENCE
    
    , 
  
    
      LAST_UPDATED_DATE
    
    , 
  
    
      LAST_REPORTED_FLAG
    
    
  
)

  copy grants as (
    with raw_source as (

    select *
    from starschema_covid19.public.JHU_COVID_19

),

final as (

    select
        "COUNTRY_REGION" as country_region,
        "PROVINCE_STATE" as province_state,
        "COUNTY" as county,
        "FIPS" as fips,
        "DATE" as date,
        "CASE_TYPE" as case_type,
        "CASES" as cases,
        "LONG" as long,
        "LAT" as lat,
        "ISO3166_1" as iso3166_1,
        "ISO3166_2" as iso3166_2,
        "DIFFERENCE" as difference,
        "LAST_UPDATED_DATE" as last_updated_date,
        "LAST_REPORTED_FLAG" as last_reported_flag

    from raw_source

)

select * from final
  );
