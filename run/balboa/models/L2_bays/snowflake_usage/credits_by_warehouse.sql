
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.credits_by_warehouse
  
    
    
(
  
    "START_TIME" COMMENT $$$$, 
  
    "CREDITS_USED" COMMENT $$$$, 
  
    "WAREHOUSE_NAME" COMMENT $$$$
  
)

  copy grants as (
    select
    start_time,
    credits_used,
    warehouse_name
from
    BALBOA_STAGING.l2_snowflake_usage.int_warehouse_metering_history
where
    datediff(month, start_time, current_date) >= 1
  );
