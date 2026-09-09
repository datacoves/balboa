
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from BALBOA.L2_VENDORS.dim_vendors
    group by status

)

select *
from all_values
where value_field not in (
    'active','inactive'
)


