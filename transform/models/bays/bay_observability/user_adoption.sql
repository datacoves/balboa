select
    query_id,
    database_name,
    schema_name,
    warehouse_name,
    role,
    query_type,
    user_name,
    start_time,
    end_time
from {{ref ('stg_query_history')}}