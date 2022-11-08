
  create or replace  view BALBOA_STAGING.l1_country_data._airbyte_raw_country_populations
  
    
    
(
  
    "COUNTRY_CODE" COMMENT $$$$, 
  
    "COUNTRY_NAME" COMMENT $$$$, 
  
    "VALUE" COMMENT $$$$, 
  
    "YEAR" COMMENT $$$$, 
  
    "_AIRBYTE_AB_ID" COMMENT $$$$, 
  
    "_AIRBYTE_EMITTED_AT" COMMENT $$$$
  
)

  copy grants as (
    with raw_source as (

    select
        parse_json(replace(_airbyte_data::string, '"NaN"', 'null')) as airbyte_data_clean,
        *
    from raw.raw._AIRBYTE_RAW_COUNTRY_POPULATIONS

),

final as (

    select
        airbyte_data_clean:"Country Code"::varchar as country_code,
        airbyte_data_clean:"Country Name"::varchar as country_name,
        airbyte_data_clean:"Value"::varchar as value,
        airbyte_data_clean:"Year"::varchar as year,
        "_AIRBYTE_AB_ID" as _airbyte_ab_id,
        "_AIRBYTE_EMITTED_AT" as _airbyte_emitted_at

    from raw_source

)

select * from final
order by country_code
  );
