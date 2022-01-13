select 
    pipe_id,
    pipe_name,
    start_time,
    end_time,
    credits_used,
    bytes_inserted,
    files_inserted,
    to_date(start_time) as start_date,
    datediff(hour, start_time, end_time) as pipeline_operation_hours,
    hour(start_time) as time_of_day
from {{ ref('pipe_usage_history') }}
order by to_date(start_time) desc