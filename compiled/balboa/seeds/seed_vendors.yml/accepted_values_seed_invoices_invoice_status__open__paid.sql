
    
    

with all_values as (

    select
        invoice_status as value_field,
        count(*) as n_records

    from BALBOA.SEEDS.seed_invoices
    group by invoice_status

)

select *
from all_values
where value_field not in (
    'open','paid'
)


