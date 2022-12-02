
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.storage_usage_m
  
    
    
(
  
    "STORAGE" COMMENT $$$$, 
  
    "USAGE_MONTH" COMMENT $$$$
  
)

  copy grants as (
    select
    sum(total_billable_storage_tb) as storage,
    usage_month
from BALBOA_STAGING.l2_snowflake_usage.int_storage_usage
where
    datediff(month, usage_month, current_date) <= 12 and datediff(month, usage_month, current_date) >= 1
group by usage_month
order by usage_month
  );
