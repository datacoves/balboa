
  create or replace  view BALBOA_STAGING.l1_starschema_covid19.jhu_covid_19
  
    
    
(
  
    "COUNTRY_REGION" COMMENT $$The name of the country or region where the data was collected$$, 
  
    "PROVINCE_STATE" COMMENT $$The name of the province or state where the data was collected$$, 
  
    "COUNTY" COMMENT $$The name of the county where the data was collected$$, 
  
    "FIPS" COMMENT $$Federal Information Processing Standards (FIPS) code for the county where the data was collected$$, 
  
    "DATE" COMMENT $$The date when the data was collected$$, 
  
    "CASE_TYPE" COMMENT $$The type of COVID-19 case (confirmed, deaths, recovered)$$, 
  
    "CASES" COMMENT $$The number of COVID-19 cases for a given location and date$$, 
  
    "LONG" COMMENT $$The longitude coordinate of the location where the data was collected$$, 
  
    "LAT" COMMENT $$The latitude coordinate of the location where the data was collected$$, 
  
    "ISO3166_1" COMMENT $$The ISO 3166-1 alpha-2 code for the country where the data was collected$$, 
  
    "ISO3166_2" COMMENT $$The ISO 3166-2 code for the country where the data was collected$$, 
  
    "DIFFERENCE" COMMENT $$The difference in case numbers from the previous day's data$$, 
  
    "LAST_UPDATED_DATE" COMMENT $$The date when the data was last updated$$, 
  
    "LAST_REPORTED_FLAG" COMMENT $$A flag indicating whether the data is the most recently reported for a given location and date$$
  
)

  copy grants as (
    with raw_source as (
    select * from starschema_covid19.public.JHU_COVID_19
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

select *
from final
  );
