select
    start_time,
    credits_used,
    warehouse_name
from
    BALBOA.l2_snowflake_usage.int_warehouse_metering_history
where
    datediff(month, start_time, current_date) >= 1