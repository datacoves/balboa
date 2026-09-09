
    
    

select
    po_id as unique_field,
    count(*) as n_records

from BALBOA.L2_INVOICES.fct_invoices
where po_id is not null
group by po_id
having count(*) > 1


