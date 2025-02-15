
        

    
        

    create or replace dynamic table BALBOA.L3_LOAN_ANALYTICS.loans_by_state__dynamic
        target_lag = '30 days'
        warehouse = wh_transforming_dynamic_tables
        as (
            

select
    personal_loans.addr_state as state,
    state_codes.state_name,
    count(*) as number_of_loans

from BALBOA.L1_LOANS.stg_personal_loans as personal_loans
join BALBOA.SEEDS.state_codes as state_codes
    on personal_loans.addr_state = state_codes.state_code
group by 1, 2
order by 2 desc
limit 10
        )
    ;
    alter dynamic table BALBOA.L3_LOAN_ANALYTICS.loans_by_state__dynamic refresh



    


    