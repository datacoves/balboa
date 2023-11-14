
  create or replace   view BALBOA_STAGING.l2_covid_observations.base_cases
  
    
    
(
  
    "COUNTRY_REGION" COMMENT $$The name of the country or region where the COVID-19 cases were reported$$, 
  
    "PROVINCE_STATE" COMMENT $$The name of the province or state where the COVID-19 cases were reported$$, 
  
    "COUNTY" COMMENT $$The name of the county where the COVID-19 cases were reported$$, 
  
    "FIPS" COMMENT $$Federal Information Processing Standards (FIPS) code for the county where the data was collected$$, 
  
    "DATE" COMMENT $$The date when the COVID-19 cases were reported$$, 
  
    "CASE_TYPE" COMMENT $$The type of COVID-19 case (confirmed, deaths, recovered)$$, 
  
    "CASES" COMMENT $$The number of reported COVID-19 cases for a given location and date$$, 
  
    "LONG" COMMENT $$The longitude coordinate of the location where the COVID-19 cases were reported$$, 
  
    "LAT" COMMENT $$The latitude coordinate of the location where the COVID-19 cases were reported$$, 
  
    "ISO3166_1" COMMENT $$The ISO 3166-1 alpha-2 code for the country where the COVID-19 cases were reported$$, 
  
    "ISO3166_2" COMMENT $$The ISO 3166-2 code for the country where the COVID-19 cases were reported$$, 
  
    "DIFFERENCE" COMMENT $$The difference in case numbers from the previous day's data$$, 
  
    "LAST_UPDATED_DATE" COMMENT $$The date when the data was last updated$$, 
  
    "LAST_REPORTED_FLAG" COMMENT $$A flag indicating whether the data is the most recently reported for a given location and date$$, 
  
    "NEW_CASES" COMMENT $$The number of new COVID-19 cases reported for a given location and date$$
  
)

  copy grants as (
    with final as (

    select
        *,
        difference as new_cases
    from l1_covid19_epidemiological_data.jhu_covid_19

)

select * from final
  );

