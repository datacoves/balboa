
    
    

select
    invoice_id as unique_field,
    count(*) as n_records

from BALBOA.L1_ERP.stg_invoices
where invoice_id is not null
group by invoice_id
having count(*) > 1


