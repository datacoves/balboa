





with validation_errors as (

    select
        period_grain, period_end_date, vendor_id
    from BALBOA.L3_ACCOUNTS_PAYABLE.mart_active_vendors_by_period
    group by period_grain, period_end_date, vendor_id
    having count(*) > 1

)

select *
from validation_errors


