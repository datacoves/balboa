
  create or replace  view staging_BALBOA.bay_observability.storage_usage_m
  
    
    
(
  
    
      STORAGE
    
    , 
  
    
      USAGE_MONTH
    
    
  
)

  copy grants as (
    select
    sum(total_billable_storage_tb) as storage,
    usage_month
from bay_observability.stg_storage_usage
where
    datediff(month, usage_month, current_date) <= 12 and datediff(month, usage_month, current_date) >= 1
group by usage_month
order by usage_month
  );
