
  create or replace  view BALBOA_STAGING.source_account_usage.pipe_usage_history
  
    
    
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
    
    
  
)

  copy grants as (
    with raw_source as (

    select * from snowflake.account_usage.PIPE_USAGE_HISTORY

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
