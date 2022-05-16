
  create or replace  view staging_BALBOA.bay_observability.credits_by_warehouse 
  
    
    
(
  
    
      MONTH_N
    
    , 
  
    
      MONTHLY_CREDITS
    
    , 
  
    
      WAREHOUSE_NAME
    
    
  
)

  copy grants as (
    select
    date_trunc(month, start_time) as month_n,
    sum(credits_used) as monthly_credits,
    warehouse_name
from
    bay_observability.stg_warehouse_metering_history
where
    start_time >= dateadd(year, -1, date_trunc(month, current_date))
    and start_time < date_trunc(month, current_date)
group by
    month_n, warehouse_name
order by
    month_n asc
  );
