
    
    

select
    vendor_id as unique_field,
    count(*) as n_records

from BALBOA.L3_FINANCE.mart_vendor_activity
where vendor_id is not null
group by vendor_id
having count(*) > 1


