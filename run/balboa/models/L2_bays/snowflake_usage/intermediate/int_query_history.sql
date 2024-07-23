
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.int_query_history
  
    
    
(
  
    "QUERY_ID" COMMENT $$$$, 
  
    "DATABASE_NAME" COMMENT $$$$, 
  
    "SCHEMA_NAME" COMMENT $$$$, 
  
    "WAREHOUSE_NAME" COMMENT $$$$, 
  
    "QUERY_TIME" COMMENT $$$$, 
  
    "ROLE" COMMENT $$$$, 
  
    "USER_NAME" COMMENT $$$$, 
  
    "QUERY_STATUS" COMMENT $$$$, 
  
    "START_TIME" COMMENT $$$$
  
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
from L1_ACCOUNT_USAGE.query_history
  );

