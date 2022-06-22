
  create or replace  view staging_BALBOA.source_account_usage.warehouse_metering_history
  
    
    
(
  
    
      START_TIME
    
    , 
  
    
      END_TIME
    
    , 
  
    
      WAREHOUSE_ID
    
    , 
  
    
      WAREHOUSE_NAME
    
    , 
  
    
      CREDITS_USED
    
    , 
  
    
      CREDITS_USED_COMPUTE
    
    , 
  
    
      CREDITS_USED_CLOUD_SERVICES
    
    
  
)

  copy grants as (
    with raw_source as (

    select *
    from snowflake.account_usage.WAREHOUSE_METERING_HISTORY

),

final as (

    select
        "START_TIME" as start_time,
        "END_TIME" as end_time,
        "WAREHOUSE_ID" as warehouse_id,
        "WAREHOUSE_NAME" as warehouse_name,
        "CREDITS_USED" as credits_used,
        "CREDITS_USED_COMPUTE" as credits_used_compute,
        "CREDITS_USED_CLOUD_SERVICES" as credits_used_cloud_services

    from raw_source

)

select * from final
  );
