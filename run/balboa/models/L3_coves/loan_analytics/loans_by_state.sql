
        
    drop dynamic table if exists "BALBOA"."L3_LOAN_ANALYTICS"."LOANS_BY_STATE"
;
    create or replace dynamic table BALBOA.l3_loan_analytics.loans_by_state
        target_lag = '190 days'
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
    alter dynamic table BALBOA.l3_loan_analytics.loans_by_state refresh
    