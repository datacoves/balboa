
    
    

with all_values as (

    select
        activity_bucket as value_field,
        count(*) as n_records

    from BALBOA.L1_ERP.stg_vendors
    group by activity_bucket

)

select *
from all_values
where value_field not in (
    'A','B','C','D'
)


