
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
  
    
    
(
  
    "START_TIME" COMMENT $$$$, 
  
    "END_TIME" COMMENT $$$$, 
  
    "WAREHOUSE_ID" COMMENT $$$$, 
  
    "WAREHOUSE_NAME" COMMENT $$$$, 
  
    "CREDITS_USED" COMMENT $$$$, 
  
    "START_DATE" COMMENT $$$$, 
  
    "WAREHOUSE_OPERATION_HOURS" COMMENT $$$$, 
  
    "TIME_OF_DAY" COMMENT $$$$
  
)

  copy grants as (
    select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    month(start_time) as start_date,
    datediff(hour, start_time, end_time) as warehouse_operation_hours,
    hour(start_time) as time_of_day
from L1_ACCOUNT_USAGE.warehouse_metering_history
  );

