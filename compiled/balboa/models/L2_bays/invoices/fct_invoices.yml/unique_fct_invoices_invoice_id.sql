
    
    

select
    invoice_id as unique_field,
    count(*) as n_records

from BALBOA.L2_INVOICES.fct_invoices
where invoice_id is not null
group by invoice_id
having count(*) > 1


