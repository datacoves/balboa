select
    start_time,
    credits_used,
    warehouse_name
from
    {{ ref('stg_warehouse_metering_history') }}
where
    datediff(month, start_time, current_date) >= 1
