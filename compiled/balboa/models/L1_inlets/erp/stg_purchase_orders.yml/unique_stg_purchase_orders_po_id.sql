
    
    

select
    po_id as unique_field,
    count(*) as n_records

from BALBOA.L1_ERP.stg_purchase_orders
where po_id is not null
group by po_id
having count(*) > 1


