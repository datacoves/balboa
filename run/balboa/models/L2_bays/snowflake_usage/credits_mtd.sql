
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.credits_mtd
  
    
    
(
  
    "MTD_CREDITS_USED" COMMENT $$The number of credits used so far in the current month$$, 
  
    "PREVIOUS_MTD_CREDITS_USED" COMMENT $$The number of credits used in the previous month up to the same date$$
  
)

  copy grants as (
    select
    credits_used as mtd_credits_used,
    (
        select sum(credits_used) as credits_used_sum
        from
            L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
        where
            timestampdiff(month, start_time, current_date) = 1
            and day(current_date) >= day(start_time)
    ) as previous_mtd_credits_used
from
    L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
where
    timestampdiff(month, start_time, current_date) = 0
  );

