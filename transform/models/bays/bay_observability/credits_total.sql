select sum(credits_used) as credits_used
from
    {{ ref('stg_warehouse_metering_history') }}
where
    datediff(month, start_time, current_date) <= 12 and datediff(month, start_time, current_date) >= 1
