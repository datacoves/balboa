select
    start_time,
    credits_used,
    warehouse_name
from
    BALBOA.L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
where
    datediff(month, start_time, current_date) >= 1