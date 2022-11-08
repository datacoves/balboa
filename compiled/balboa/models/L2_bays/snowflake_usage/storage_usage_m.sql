select
    sum(total_billable_storage_tb) as storage,
    usage_month
from BALBOA.l2_snowflake_usage.int_storage_usage
where
    datediff(month, usage_month, current_date) <= 12 and datediff(month, usage_month, current_date) >= 1
group by usage_month
order by usage_month