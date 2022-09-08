
  create or replace  view BALBOA_STAGING.bay_observability.stg_query_history
  
    
    
(
  
    
      QUERY_ID
    
    , 
  
    
      DATABASE_NAME
    
    , 
  
    
      SCHEMA_NAME
    
    , 
  
    
      WAREHOUSE_NAME
    
    , 
  
    
      QUERY_TIME
    
    , 
  
    
      ROLE
    
    , 
  
    
      USER_NAME
    
    , 
  
    
      QUERY_STATUS
    
    , 
  
    
      START_TIME
    
    
  
)

  copy grants as (
    select
    query_id,
    database_name,
    schema_name,
    warehouse_name,
    total_elapsed_time as query_time,
    role_name as role,
    user_name,
    execution_status as query_status,
    start_time
from BALBOA_STAGING.source_account_usage.query_history
  );
