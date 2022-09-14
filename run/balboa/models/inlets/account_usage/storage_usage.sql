
  create or replace  view BALBOA_STAGING.inlet_account_usage.storage_usage
  
    
    
(
  
    
      USAGE_DATE
    
    , 
  
    
      STORAGE_BYTES
    
    , 
  
    
      STAGE_BYTES
    
    , 
  
    
      FAILSAFE_BYTES
    
    
  
)

  copy grants as (
    with raw_source as (

    select *
    from snowflake.account_usage.STORAGE_USAGE

),

final as (

    select
        "USAGE_DATE" as usage_date,
        "STORAGE_BYTES" as storage_bytes,
        "STAGE_BYTES" as stage_bytes,
        "FAILSAFE_BYTES" as failsafe_bytes

    from raw_source

)

select * from final
  );
