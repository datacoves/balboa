select
    date_trunc(month, start_time) as month_n,
    sum(credits_used) as monthly_credits,
    warehouse_name
from
    {{ ref('stg_warehouse_metering_history') }}
where
    datediff(month, start_time, current_date) <= 12 and datediff(month, start_time, current_date) >= 1
group by
    month_n, warehouse_name
order by
    month_n asc
