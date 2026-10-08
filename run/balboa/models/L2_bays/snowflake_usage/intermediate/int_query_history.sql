
  create or replace   view BALBOA_STAGING.L2_SNOWFLAKE_USAGE.int_query_history
  
    
    
(
  
    "QUERY_ID" COMMENT $$A unique identifier assigned to the query$$, 
  
    "DATABASE_NAME" COMMENT $$The name of the database where the query was executed$$, 
  
    "SCHEMA_NAME" COMMENT $$The name of the schema where the query was executed$$, 
  
    "WAREHOUSE_NAME" COMMENT $$The name of the warehouse where the query was executed$$, 
  
    "QUERY_TIME" COMMENT $$The time at which the query was executed.$$, 
  
    "ROLE" COMMENT $$The role associated with the user who executed the query.$$, 
  
    "USER_NAME" COMMENT $$The name of the user who executed the query$$, 
  
    "QUERY_STATUS" COMMENT $$The status of the executed query.$$, 
  
    "START_TIME" COMMENT $$The start time of the query execution$$
  
)

  copy grants
  
  
  as (
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
from L1_ACCOUNT_USAGE.stg_query_history
  );

