
  create or replace   view BALBOA_STAGING.L1_US_POPULATION.stg_us_population
  
    
    
(
  
    "STATE_ID" COMMENT $$The unique identifier for each record in the model.$$, 
  
    "STATE_NAME" COMMENT $$The name of the state.$$, 
  
    "2010" COMMENT $$The population count for the year 2010.$$, 
  
    "2011" COMMENT $$The population count for the year 2011.$$, 
  
    "2012" COMMENT $$The population count for the year 2012.$$, 
  
    "2013" COMMENT $$The population count for the year 2013.$$, 
  
    "2014" COMMENT $$The population count for the year 2014.$$, 
  
    "2015" COMMENT $$The population count for the year 2015.$$, 
  
    "2016" COMMENT $$The population count for the year 2016.$$, 
  
    "2017" COMMENT $$The population count for the year 2017.$$, 
  
    "2018" COMMENT $$The population count for the year 2018.$$, 
  
    "2019" COMMENT $$The population count for the year 2019.$$
  
)

  copy grants
  
  
  as (
    with raw_source as (

    select *
    from RAW.US_POPULATION.US_POPULATION

),

final as (

    select
        "ID"::number as state_id,
        "STATES"::varchar as state_name,
        replace("_2010", ',', '')::integer as "2010",
        replace("_2011", ',', '')::integer as "2011",
        replace("_2012", ',', '')::integer as "2012",
        replace("_2013", ',', '')::integer as "2013",
        replace("_2014", ',', '')::integer as "2014",
        replace("_2015", ',', '')::integer as "2015",
        replace("_2016", ',', '')::integer as "2016",
        replace("_2017", ',', '')::integer as "2017",
        replace("_2018", ',', '')::integer as "2018",
        replace("_2019", ',', '')::integer as "2019"

    from raw_source

)

select * from final
  );

