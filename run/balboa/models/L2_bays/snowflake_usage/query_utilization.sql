
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.query_utilization
  
    
    
(
  
    "QUERY_ID" COMMENT $$The ID of the query$$, 
  
    "DATABASE_NAME" COMMENT $$The name of the database in which the query was run$$, 
  
    "SCHEMA_NAME" COMMENT $$The name of the schema in which the query was run$$, 
  
    "WAREHOUSE_NAME" COMMENT $$The name of the warehouse on which the query was run$$, 
  
    "QUERY_TIME" COMMENT $$The total time taken by the query$$, 
  
    "ROLE" COMMENT $$The role of the user running the query$$, 
  
    "USER_NAME" COMMENT $$The name of the user running the query$$, 
  
    "QUERY_STATUS" COMMENT $$The status of the query (success/failure)$$, 
  
    "START_TIME" COMMENT $$The start time of the query$$, 
  
    "QUERY_FAIL_PERCENTAGE" COMMENT $$The percentage of failed queries out of the total queries run$$, 
  
    "AVG_QUERIES_PER_USER" COMMENT $$The average number of queries run per user$$
  
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
