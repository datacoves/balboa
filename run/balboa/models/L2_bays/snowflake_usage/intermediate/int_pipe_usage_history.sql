
  create or replace   view BALBOA_STAGING.l2_snowflake_usage.int_pipe_usage_history
  
    
    
(
  
    "PIPE_ID" COMMENT $$$$, 
  
    "PIPE_NAME" COMMENT $$$$, 
  
    "START_TIME" COMMENT $$$$, 
  
    "END_TIME" COMMENT $$$$, 
  
    "CREDITS_USED" COMMENT $$$$, 
  
    "BYTES_INSERTED" COMMENT $$$$, 
  
    "FILES_INSERTED" COMMENT $$$$, 
  
    "START_DATE" COMMENT $$$$, 
  
    "PIPELINE_OPERATION_HOURS" COMMENT $$$$, 
  
    "TIME_OF_DAY" COMMENT $$$$
  
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
from l1_account_usage.pipe_usage_history
order by to_date(start_time) desc
  );

