
  create or replace   view BALBOA_STAGING.L1_COUNTRY_DATA.stg_country_populations_v2
  
    
    
(
  
    "YEAR" COMMENT $$The year for which the population value is recorded$$, 
  
    "LAST_YEAR" COMMENT $$The prior year for which the population value is recorded$$, 
  
    "COUNTRY_NAME" COMMENT $$The name of the country$$, 
  
    "VALUE" COMMENT $$The population value for a particular year and country$$, 
  
    "COUNTRY_CODE" COMMENT $$The ISO 3166-1 alpha-2 code for the country$$
  
)

  copy grants
  
  
  as (
    with raw_source as (

    select *
    from RAW.RAW.COUNTRY_POPULATIONS

),

final as (

    select
        year,
        year - 1 as last_year,
        "COUNTRY NAME" as country_name,
        value,
        "COUNTRY CODE" as country_code

    from raw_source

)

select * from final
order by country_code
  );

