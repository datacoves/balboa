
  create or replace  view BALBOA_STAGING.l2_snowflake_usage.credits_variance
  
    
    
(
  
    "MONTH_N" COMMENT $$The current month (1-12) for which credit usage is being reported$$, 
  
    "MONTH_D" COMMENT $$The previous month (1-12) for which credit usage is being reported$$, 
  
    "PREV_MONTH" COMMENT $$The name of the previous month$$, 
  
    "MONTHLY_CREDITS" COMMENT $$The total number of credits used in the current month$$, 
  
    "PREV_MONTHLY_CREDITS" COMMENT $$The total number of credits used in the previous month$$, 
  
    "DIFFERENCE" COMMENT $$The difference in credits used between the current and previous months$$, 
  
    "VARIANCE" COMMENT $$The percentage variance in credits used between the current and previous months$$
  
)

  copy grants as (
    with this_month as (
    select
        case
            when timestampdiff(month, start_time, current_date) = 1 then 1
            when timestampdiff(month, start_time, current_date) = 2 then 2
            when timestampdiff(month, start_time, current_date) = 3 then 3
            when timestampdiff(month, start_time, current_date) = 4 then 4
            when timestampdiff(month, start_time, current_date) = 5 then 5
            when timestampdiff(month, start_time, current_date) = 6 then 6
            when timestampdiff(month, start_time, current_date) = 7 then 7
            when timestampdiff(month, start_time, current_date) = 8 then 8
            when timestampdiff(month, start_time, current_date) = 9 then 9
            when timestampdiff(month, start_time, current_date) = 10 then 10
            when timestampdiff(month, start_time, current_date) = 11 then 11
            when timestampdiff(month, start_time, current_date) = 12 then 12
            when timestampdiff(month, start_time, current_date) = 13 then 13
        end as month_n,
        date_trunc(month, start_time) as month_d,
        sum(credits_used) as monthly_credits
    from
        l2_snowflake_usage.int_warehouse_metering_history
    where
        start_time >= dateadd(month, -13, date_trunc(month, current_date))
        and start_time < date_trunc(month, current_date)
    group by
        month_d, month_n
    order by
        month_d asc
),

prev_month as (
    select
        case
            when timestampdiff(month, start_time, current_date) = 1 then 0
            when timestampdiff(month, start_time, current_date) = 2 then 1
            when timestampdiff(month, start_time, current_date) = 3 then 2
            when timestampdiff(month, start_time, current_date) = 4 then 3
            when timestampdiff(month, start_time, current_date) = 5 then 4
            when timestampdiff(month, start_time, current_date) = 6 then 5
            when timestampdiff(month, start_time, current_date) = 7 then 6
            when timestampdiff(month, start_time, current_date) = 8 then 7
            when timestampdiff(month, start_time, current_date) = 9 then 8
            when timestampdiff(month, start_time, current_date) = 10 then 9
            when timestampdiff(month, start_time, current_date) = 11 then 10
            when timestampdiff(month, start_time, current_date) = 12 then 11
            when timestampdiff(month, start_time, current_date) = 13 then 12
        end as prev_month,
        date_trunc(month, start_time) as prev_month_d,
        sum(credits_used) as prev_monthly_credits
    from
        l2_snowflake_usage.int_warehouse_metering_history
    where
        start_time >= dateadd(month, -13, date_trunc(month, current_date))
        and start_time < date_trunc(month, current_date)
    group by
        prev_month_d, prev_month
    order by
        prev_month_d asc
)

select
    this_month.month_n,
    this_month.month_d,
    prev_month.prev_month,
    this_month.monthly_credits,
    prev_month.prev_monthly_credits,
    this_month.monthly_credits - prev_month.prev_monthly_credits as difference,
    sum(difference) over (order by this_month.month_n desc rows between unbounded preceding and current row) as variance
from this_month
left join prev_month on this_month.month_n = prev_month.prev_month
  );
