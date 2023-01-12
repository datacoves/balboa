
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.int_storage_usage
  
    
    
(
  
    "USAGE_MONTH" COMMENT $$$$, 
  
    "TOTAL_BILLABLE_STORAGE_TB" COMMENT $$$$, 
  
    "STORAGE_BILLABLE_STORAGE_TB" COMMENT $$$$, 
  
    "STAGE_BILLABLE_STORAGE_TB" COMMENT $$$$, 
  
    "FAILSAFE_BILLABLE_STORAGE_TB" COMMENT $$$$
  
)

  copy grants as (
    select
    date_trunc(month, usage_date) as usage_month,
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as total_billable_storage_tb,
    avg(storage_bytes) / power(1024, 4) as storage_billable_storage_tb,
    avg(stage_bytes) / power(1024, 4) as stage_billable_storage_tb,
    avg(failsafe_bytes) / power(1024, 4) as failsafe_billable_storage_tb
from l1_account_usage.storage_usage
group by date_trunc(month, usage_date)
order by date_trunc(month, usage_date)
  );
