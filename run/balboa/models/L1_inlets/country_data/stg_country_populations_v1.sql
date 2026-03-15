
  create or replace   view BALBOA_STAGING.L1_COUNTRY_DATA.stg_country_populations_v1
  
    
    
(
  
    "YEAR" COMMENT $$The year for which the population value is recorded$$, 
  
    "COUNTRY_NAME" COMMENT $$The name of the country$$, 
  
    "VALUE" COMMENT $$The population value for a particular year and country$$, 
  
    "COUNTRY_CODE" COMMENT $$The alpha-3 code for the country$$
  
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
        "COUNTRY_NAME" as country_name,
        value,
        "COUNTRY_CODE" as country_code

    from raw_source

)

select * from final
order by country_code
  );

