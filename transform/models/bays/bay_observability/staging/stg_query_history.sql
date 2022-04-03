select
    query_id,
    database_id,
    database_name,
    schema_id,
    schema_name,
    warehouse_id,
    warehouse_name,
    total_elapsed_time as query_time,
    role_name as role,
    query_type,
    user_name,
    execution_status as query_status,
    start_time,
    end_time
from {{ref ('query_history')}}