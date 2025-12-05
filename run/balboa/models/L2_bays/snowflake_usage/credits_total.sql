
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.credits_total
  
    
    
(
  
    "CREDITS_USED" COMMENT $$The total number of credits used during the specified period$$, 
  
    "START_TIME" COMMENT $$The start time for the period during which credits were used$$, 
  
    "RANK" COMMENT $$The rank of the specified period in descending order of credits used$$, 
  
    "DAY_NAME" COMMENT $$The day of the week for the start time of the specified period$$, 
  
    "TOD" COMMENT $$The time of day for the start time of the specified period$$
  
)

  copy grants
  
  
  as (
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
    L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
  );

