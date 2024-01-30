
        
    drop dynamic table if exists "BALBOA"."L3_LOAN_ANALYTICS"."LOANS_BY_STATE__DYNAMIC"
;
    create or replace dynamic table balboa.l3_loan_analytics.loans_by_state__dynamic
        target_lag = '1 minute'
        warehouse = wh_transforming
        as (
            

select
    personal_loans.addr_state as state,
    state_codes.state_name,
    count(*) as number_of_loans

from balboa.l1_loans.personal_loans as personal_loans
join balboa.seeds.state_codes as state_codes
    on personal_loans.addr_state = state_codes.state_code
group by 1, 2
order by 2 desc
limit 10
        )
    ;
    alter dynamic table balboa.l3_loan_analytics.loans_by_state__dynamic refresh
    