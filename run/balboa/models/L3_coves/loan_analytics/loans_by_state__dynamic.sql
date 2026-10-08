
        

    
        create or replace transient dynamic table BALBOA_STAGING.L3_LOAN_ANALYTICS.loans_by_state__dynamic
    target_lag = '30 days'
    warehouse = wh_transforming_dynamic_tables
    

    refresh_mode = AUTO

    initialize = ON_CREATE

    
    scheduler = 'ENABLE'
    
    

    

    

    copy grants
    

    as (
        

select
    personal_loans.addr_state as state,
    state_codes.state_name,
    count(*) as number_of_loans

from L1_LOANS.stg_personal_loans as personal_loans
join SEEDS.state_codes as state_codes
    on personal_loans.addr_state = state_codes.state_code
group by state, state_name
order by state_name desc
limit 10
    )

    


    