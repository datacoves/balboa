select
    sum(total_billable_storage_tb) as storage,
    usage_month
from {{ ref('stg_storage_usage') }}
where
    usage_month >= dateadd(year, -1, date_trunc(month, current_date)) and usage_month < date_trunc(month, current_date)
group by usage_month
order by usage_month

