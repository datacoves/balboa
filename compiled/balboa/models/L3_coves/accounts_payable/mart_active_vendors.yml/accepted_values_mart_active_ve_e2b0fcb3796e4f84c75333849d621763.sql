
    
    

with all_values as (

    select
        recorded_status as value_field,
        count(*) as n_records

    from BALBOA.L3_ACCOUNTS_PAYABLE.mart_active_vendors_by_period
    group by recorded_status

)

select *
from all_values
where value_field not in (
    'active','inactive'
)


