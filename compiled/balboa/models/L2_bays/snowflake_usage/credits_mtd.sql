select
    credits_used as mtd_credits_used,
    (
        select sum(credits_used) as credits_used_sum
        from
            BALBOA.L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
        where
            timestampdiff(month, start_time, current_date) = 1
            and day(current_date) >= day(start_time)
    ) as previous_mtd_credits_used
from
    BALBOA.L2_SNOWFLAKE_USAGE.int_warehouse_metering_history
where
    timestampdiff(month, start_time, current_date) = 0