
    select
        date_trunc(month, start_time) as month_n,
        credits_used
    from {{ ref('stg_warehouse_metering_history') }}
    where
        start_time >= dateadd(year, -1, date_trunc(month, current_date)) and start_time < date_trunc(month, current_date)
    group by month_n
    order by month_n asc


-- select
--     month_n,
--     sum(monthly_credits) over(order by month_n asc rows between unbounded preceding and current row) as cumulative_sum
-- from
--     cte
-- order by month_n asc
