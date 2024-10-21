
  
    

        create or replace transient table BALBOA_STAGING.L3_LOAN_ANALYTICS.loans_by_state__standard
        copy grants as
        (

select
    personal_loans.addr_state as state,
    state_codes.state_name,
    count(*) as number_of_loans

from L1_LOANS.personal_loans as personal_loans
join SEEDS.state_codes as state_codes
    on personal_loans.addr_state = state_codes.state_code
group by 1, 2
order by 2 desc
limit 10
        );
      
  