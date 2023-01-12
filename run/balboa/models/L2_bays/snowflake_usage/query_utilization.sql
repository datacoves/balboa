
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.query_utilization
  
    
    
(
  
    "QUERY_ID" COMMENT $$$$, 
  
    "DATABASE_NAME" COMMENT $$$$, 
  
    "SCHEMA_NAME" COMMENT $$$$, 
  
    "WAREHOUSE_NAME" COMMENT $$$$, 
  
    "QUERY_TIME" COMMENT $$$$, 
  
    "ROLE" COMMENT $$$$, 
  
    "USER_NAME" COMMENT $$$$, 
  
    "QUERY_STATUS" COMMENT $$$$, 
  
    "START_TIME" COMMENT $$$$, 
  
    "QUERY_FAIL_PERCENTAGE" COMMENT $$$$, 
  
    "AVG_QUERIES_PER_USER" COMMENT $$$$
  
)

  copy grants as (
    with query_fail as (
    select count_if(query_status like 'FAIL') / count(query_status) * 100 as query_fail_percentage
    from l2_snowflake_usage.int_query_history
),

queries_per_user as (
    select count(query_id) / count(distinct user_name) as queries
    from l2_snowflake_usage.int_query_history
)
select
    query_id,
    database_name,
    schema_name,
    warehouse_name,
    query_time,
    role,
    user_name,
    query_status,
    start_time,
    (select * from query_fail) as query_fail_percentage,
    (select * from queries_per_user) as avg_queries_per_user
from l2_snowflake_usage.int_query_history
  );
