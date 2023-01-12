
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.credits_total
  
    
    
(
  
    "CREDITS_USED" COMMENT $$$$, 
  
    "START_TIME" COMMENT $$$$, 
  
    "RANK" COMMENT $$$$, 
  
    "DAY_NAME" COMMENT $$$$, 
  
    "TOD" COMMENT $$$$
  
)

  copy grants as (
    select
    credits_used,
    start_time,
    case
        when dayname(start_time) like 'Mon' then 1
        when dayname(start_time) like 'Tue' then 2
        when dayname(start_time) like 'Wed' then 3
        when dayname(start_time) like 'Thu' then 4
        when dayname(start_time) like 'Fri' then 5
        when dayname(start_time) like 'Sat' then 6
        when dayname(start_time) like 'Sun' then 7
    end as rank,
    dayname(start_time) as day_name,
    hour(start_time) as tod
from
    l2_snowflake_usage.int_warehouse_metering_history
  );
