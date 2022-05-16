
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
    usage_month >= dateadd(year, -1, date_trunc(month, current_date)) and usage_month < date_trunc(month, current_date)
group by usage_month
order by usage_month
  );
