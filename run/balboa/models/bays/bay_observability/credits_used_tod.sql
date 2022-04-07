
  create or replace  view staging_BALBOA.bay_observability.credits_used_tod 
  
    
    
(
  
    
      CREDITS_USED
    
    , 
  
    
      HOUR
    
    
  
)

  copy grants as (
    select
    sum(credits_used) as credits_used,
    hour(start_time) as hour
from 
    
        bay_observability.stg_warehouse_metering_history
    

group by hour
order by hour
  );
