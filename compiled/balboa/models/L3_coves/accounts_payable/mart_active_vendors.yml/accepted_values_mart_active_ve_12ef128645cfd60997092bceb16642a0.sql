
    
    

with all_values as (

    select
        period_grain as value_field,
        count(*) as n_records

    from BALBOA.L3_ACCOUNTS_PAYABLE.mart_active_vendors_by_period
    group by period_grain

)

select *
from all_values
where value_field not in (
    'week','month','quarter'
)


