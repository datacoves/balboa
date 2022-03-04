select
    sum(credits_used) as credits_used,
    hour(start_time) as hour
from {{ ref('stg_warehouse_metering_history') }}
group by hour
order by hour
