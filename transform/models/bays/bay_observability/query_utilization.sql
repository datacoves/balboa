SELECT
    query_id,
    database_id,
    database_name,
    schema_id,
    schema_name,
    warehouse_id,
    warehouse_name,
    query_time,
    role,
    query_type,
    user_name,
    query_status
from {{ref('stg_query_history')}}