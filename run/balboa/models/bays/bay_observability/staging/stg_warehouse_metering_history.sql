
  create or replace  view staging_BALBOA.bay_observability.stg_warehouse_metering_history 
  
    
    
(
  
    
      START_TIME
    
    , 
  
    
      END_TIME
    
    , 
  
    
      WAREHOUSE_ID
    
    , 
  
    
      WAREHOUSE_NAME
    
    , 
  
    
      CREDITS_USED
    
    , 
  
    
      START_DATE
    
    , 
  
    
      WAREHOUSE_OPERATION_HOURS
    
    , 
  
    
      TIME_OF_DAY
    
    
  
)

  copy grants as (
    select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    month(start_time) as start_date,
    datediff(hour, start_time, end_time) as warehouse_operation_hours,
    hour(start_time) as time_of_day
from source_account_usage.warehouse_metering_history
  );
