

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