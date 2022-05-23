
  create or replace  view staging_BALBOA.bay_observability.query_utilization
  
    
    
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
    
    , 
  
    
      QUERY_FAIL_PERCENTAGE
    
    , 
  
    
      AVG_QUERIES_PER_USER
    
    
  
)

  copy grants as (
    with query_fail as (
    select
        count_if(query_status like 'FAIL') / count(query_status) * 100 as query_fail_percentage
    from bay_observability.stg_query_history
),

queries_per_user as (
    select
        count(query_id) / count(distinct user_name) as queries
    from bay_observability.stg_query_history
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
from bay_observability.stg_query_history
  );
