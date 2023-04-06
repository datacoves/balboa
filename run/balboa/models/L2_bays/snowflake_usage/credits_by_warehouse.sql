
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.credits_by_warehouse
  
    
    
(
  
    "START_TIME" COMMENT $$The start time for the period during which credits were used$$, 
  
    "CREDITS_USED" COMMENT $$The number of credits used during the specified period$$, 
  
    "WAREHOUSE_NAME" COMMENT $$The name of the warehouse where credits were used$$
  
)

  copy grants as (
    select
    start_time,
    credits_used,
    warehouse_name
from
    l2_snowflake_usage.int_warehouse_metering_history
where
    datediff(month, start_time, current_date) >= 1
  );
