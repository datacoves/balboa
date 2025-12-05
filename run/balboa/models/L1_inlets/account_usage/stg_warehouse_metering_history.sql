
  create or replace   view BALBOA_STAGING.L1_ACCOUNT_USAGE.stg_warehouse_metering_history
  
    
    
(
  
    "START_TIME" COMMENT $$The start time of the warehouse metering period$$, 
  
    "END_TIME" COMMENT $$The end time of the warehouse metering period$$, 
  
    "WAREHOUSE_ID" COMMENT $$The unique identifier of the warehouse$$, 
  
    "WAREHOUSE_NAME" COMMENT $$The name of the warehouse$$, 
  
    "CREDITS_USED" COMMENT $$The total number of credits used by the warehouse$$, 
  
    "CREDITS_USED_COMPUTE" COMMENT $$The number of credits used for compute$$, 
  
    "CREDITS_USED_CLOUD_SERVICES" COMMENT $$The number of credits used for cloud services$$
  
)

  copy grants
  
  
  as (
    with raw_source as (

    select *
    from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY

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

