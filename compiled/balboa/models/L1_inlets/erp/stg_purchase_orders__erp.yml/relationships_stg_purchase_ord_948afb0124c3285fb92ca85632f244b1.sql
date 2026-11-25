
    
    

with child as (
    select vendor_id as from_field
    from BALBOA.L1_ERP.stg_purchase_orders__erp
    where vendor_id is not null
),

parent as (
    select vendor_id as to_field
    from BALBOA.L1_ERP.stg_vendors__erp
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


