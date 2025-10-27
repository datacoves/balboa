
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.int_pipe_usage_history
  
    
    
(
  
    "PIPE_ID" COMMENT $$A unique identifier assigned to the pipe$$, 
  
    "PIPE_NAME" COMMENT $$The name of the pipe used for identification purposes$$, 
  
    "START_TIME" COMMENT $$The start time when the pipe was used$$, 
  
    "END_TIME" COMMENT $$The end time when the pipe was used$$, 
  
    "CREDITS_USED" COMMENT $$The amount of credits used by the pipe during usage$$, 
  
    "BYTES_INSERTED" COMMENT $$The amount of data in bytes inserted into the pipe during usage$$, 
  
    "FILES_INSERTED" COMMENT $$The number of files inserted into the pipe during usage$$, 
  
    "START_DATE" COMMENT $$The date when the pipe usage started.$$, 
  
    "PIPELINE_OPERATION_HOURS" COMMENT $$Duration in hours that the pipeline was operational.$$, 
  
    "TIME_OF_DAY" COMMENT $$Indicates the specific time or period of the day when the pipe was used.$$
  
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
from L1_ACCOUNT_USAGE.stg_pipe_usage_history
order by to_date(start_time) desc
  );

