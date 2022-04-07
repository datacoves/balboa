
  create or replace  view staging_BALBOA.bay_observability.credits_total 
  
    
    
(
  
    
      CREDITS_USED
    
    
  
)

  copy grants as (
    select sum(credits_used) as credits_used
from
    
    
        bay_observability.stg_warehouse_metering_history
    

where
    timestampdiff(month, start_time, current_date) <= 12 and timestampdiff(month, start_time, current_date) > 0
  );
