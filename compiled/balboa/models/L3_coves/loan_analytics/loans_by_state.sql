

select
    addr_state as state,
    count(*) as number_of_loans
from
    RAW.LOANS.PERSONAL_LOANS
group by 1
order by 2 desc
limit 10