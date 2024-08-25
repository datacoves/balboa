
    
    

with all_values as (

    select
        addr_state as value_field,
        count(*) as n_records

    from BALBOA.L1_LOANS.personal_loans
    group by addr_state

)

select *
from all_values
where value_field not in (
    'CA'
)


