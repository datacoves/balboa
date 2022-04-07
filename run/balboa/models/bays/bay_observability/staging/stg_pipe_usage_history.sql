
  create or replace  view staging_BALBOA.bay_observability.stg_pipe_usage_history 
  
    
    
(
  
    
      PIPE_ID
    
    , 
  
    
      PIPE_NAME
    
    , 
  
    
      START_TIME
    
    , 
  
    
      END_TIME
    
    , 
  
    
      CREDITS_USED
    
    , 
  
    
      BYTES_INSERTED
    
    , 
  
    
      FILES_INSERTED
    
    , 
  
    
      START_DATE
    
    , 
  
    
      PIPELINE_OPERATION_HOURS
    
    , 
  
    
      TIME_OF_DAY
    
    
  
)

  copy grants as (
    select
    pipe_id,
    pipe_name,
    start_time,
    end_time,
    credits_used,
    bytes_inserted,
    files_inserted,
    to_date(start_time) as start_date,
    datediff(hour, start_time, end_time) as pipeline_operation_hours,
    hour(start_time) as time_of_day
from 
    
        source_account_usage.pipe_usage_history
    

order by to_date(start_time) desc
  );
