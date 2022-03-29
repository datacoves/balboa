select
    query_id,
    database_id,
    database_name,
    schema_id,
    schema_name,
    warehouse_id,
    warehouse_name,
    total_elapsed_time,
    role_name,
    query_type,
    user_name,
    execution_status
from {{ref ('query_history')}}