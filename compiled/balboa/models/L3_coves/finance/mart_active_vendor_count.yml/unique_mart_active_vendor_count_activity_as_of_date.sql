
    
    

select
    activity_as_of_date as unique_field,
    count(*) as n_records

from BALBOA.L3_FINANCE.mart_active_vendor_count
where activity_as_of_date is not null
group by activity_as_of_date
having count(*) > 1


