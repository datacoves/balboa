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
from BALBOA.l1_account_usage.query_history