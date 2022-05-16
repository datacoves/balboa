
  create or replace  view staging_BALBOA.bay_observability.credits_used_dow 
  
    
    
(
  
    
      CREDITS_USED
    
    , 
  
    
      DAY
    
    
  
)

  copy grants as (
    select
    sum(credits_used) as credits_used,
    dayname(start_time) as day
from bay_observability.stg_warehouse_metering_history
group by day
order by day
  );
