

select
    personal_loans.addr_state as state,
    state_codes.state_name,
    count(*) as number_of_loans

from BALBOA.L1_LOANS.stg_personal_loans as personal_loans
join BALBOA.SEEDS.state_codes as state_codes
    on personal_loans.addr_state = state_codes.state_code
group by state, state_name
order by state_name desc
limit 10