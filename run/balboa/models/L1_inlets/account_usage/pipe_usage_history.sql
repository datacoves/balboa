
  create or replace   view BALBOA_STAGING.L1_ACCOUNT_USAGE.pipe_usage_history
  
    
    
(
  
    "PIPE_ID" COMMENT $$A unique identifier assigned to the pipe$$, 
  
    "PIPE_NAME" COMMENT $$The name of the pipe used for identification purposes$$, 
  
    "START_TIME" COMMENT $$The start time when the pipe was used$$, 
  
    "END_TIME" COMMENT $$The end time when the pipe was used$$, 
  
    "CREDITS_USED" COMMENT $$The amount of credits used by the pipe during usage$$, 
  
    "BYTES_INSERTED" COMMENT $$The amount of data in bytes inserted into the pipe during usage$$, 
  
    "FILES_INSERTED" COMMENT $$The number of files inserted into the pipe during usage$$
  
)

  copy grants as (
    with raw_source as (

    select *
    from SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY

),

final as (
    select
        "PIPE_ID" as pipe_id,
        "PIPE_NAME" as pipe_name,
        "START_TIME" as start_time,
        "END_TIME" as end_time,
        "CREDITS_USED" as credits_used,
        "BYTES_INSERTED" as bytes_inserted,
        "FILES_INSERTED" as files_inserted
    from raw_source
)

select * from final
  );

