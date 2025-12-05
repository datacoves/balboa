
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
  
    
    
(
  
    "START_TIME" COMMENT $$The start time of the warehouse metering period$$, 
  
    "END_TIME" COMMENT $$The end time of the warehouse metering period$$, 
  
    "WAREHOUSE_ID" COMMENT $$The unique identifier of the warehouse$$, 
  
    "WAREHOUSE_NAME" COMMENT $$The name of the warehouse$$, 
  
    "CREDITS_USED" COMMENT $$The total number of credits used by the warehouse$$, 
  
    "START_DATE" COMMENT $$The start date of the warehouse metering period.$$, 
  
    "WAREHOUSE_OPERATION_HOURS" COMMENT $$The number of hours the warehouse was operational during the metering period.$$, 
  
    "TIME_OF_DAY" COMMENT $$Indicates the time of day associated with the metering data entry.$$
  
)

  copy grants
  
  
  as (
    select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    month(start_time) as start_date,
    datediff(hour, start_time, end_time) as warehouse_operation_hours,
    hour(start_time) as time_of_day
from L1_ACCOUNT_USAGE.stg_warehouse_metering_history
  );

