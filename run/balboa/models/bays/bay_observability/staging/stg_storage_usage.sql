
  create or replace  view staging_BALBOA.bay_observability.stg_storage_usage
  
    
    
(
  
    
      USAGE_MONTH
    
    , 
  
    
      TOTAL_BILLABLE_STORAGE_TB
    
    , 
  
    
      STORAGE_BILLABLE_STORAGE_TB
    
    , 
  
    
      STAGE_BILLABLE_STORAGE_TB
    
    , 
  
    
      FAILSAFE_BILLABLE_STORAGE_TB
    
    
  
)

  copy grants as (
    select
    date_trunc(month, usage_date) as usage_month,
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as total_billable_storage_tb,
    avg(storage_bytes ) / power(1024, 4) as storage_billable_storage_tb,
    avg(stage_bytes ) / power(1024, 4) as stage_billable_storage_tb,
    avg(failsafe_bytes ) / power(1024, 4) as failsafe_billable_storage_tb
from source_account_usage.storage_usage
group by date_trunc(month, usage_date)
order by date_trunc(month, usage_date)
  );
