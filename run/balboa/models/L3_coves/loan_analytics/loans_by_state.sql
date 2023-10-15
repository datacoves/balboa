
        create or replace dynamic table BALBOA_STAGING.l3_loan_analytics.loans_by_state
        target_lag = '180 days'
        warehouse = wh_transforming
        as (
            

select
    addr_state as state,
    count(*) as number_of_loans
from
    RAW.LOANS.PERSONAL_LOANS
group by 1
order by 2 desc
limit 10
        )
    ;
    alter dynamic table BALBOA_STAGING.l3_loan_analytics.loans_by_state refresh
    