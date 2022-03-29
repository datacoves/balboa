select
    start_time,
    end_time,
    warehouse_id,
    warehouse_name,
    credits_used,
    month(start_time) as start_date,
    datediff(hour, start_time, end_time) as warehouse_operation_hours,
    hour(start_time) as time_of_day
from 
    
        BALBOA.source_account_usage.warehouse_metering_history
    
