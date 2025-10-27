
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.int_storage_usage
  
    
    
(
  
    "USAGE_MONTH" COMMENT $$The month when storage usage was recorded.$$, 
  
    "TOTAL_BILLABLE_STORAGE_TB" COMMENT $$The total amount of billable storage used, measured in terabytes.$$, 
  
    "STORAGE_BILLABLE_STORAGE_TB" COMMENT $$The amount of billable storage in terabytes.$$, 
  
    "STAGE_BILLABLE_STORAGE_TB" COMMENT $$Amount of billable data in terabytes stored in the staging area.$$, 
  
    "FAILSAFE_BILLABLE_STORAGE_TB" COMMENT $$The amount of billable storage in terabytes stored in the failsafe area.$$
  
)

  copy grants as (
    select
    date_trunc(month, usage_date) as usage_month,
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as total_billable_storage_tb,
    avg(storage_bytes) / power(1024, 4) as storage_billable_storage_tb,
    avg(stage_bytes) / power(1024, 4) as stage_billable_storage_tb,
    avg(failsafe_bytes) / power(1024, 4) as failsafe_billable_storage_tb
from L1_ACCOUNT_USAGE.stg_storage_usage
group by date_trunc(month, usage_date)
order by date_trunc(month, usage_date)
  );

