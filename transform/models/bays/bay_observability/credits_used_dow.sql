select
    sum(credits_used) as credits_used,
    case
        when dayname(start_time) like 'Mon' then '1. Mon'
        when dayname(start_time) like 'Tue' then '2. Tue'
        when dayname(start_time) like 'Wed' then '3. Wed'
        when dayname(start_time) like 'Thu' then '4. Thu'
        when dayname(start_time) like 'Fri' then '5. Fri'
        when dayname(start_time) like 'Sat' then '6. Sat'
        when dayname(start_time) like 'Sun' then '7. Sun'
    end as day
from {{ ref('stg_warehouse_metering_history') }}
group by day
order by day
