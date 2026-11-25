
    
    

select
    vendor_id as unique_field,
    count(*) as n_records

from BALBOA.L1_ERP.stg_vendors__erp
where vendor_id is not null
group by vendor_id
having count(*) > 1


