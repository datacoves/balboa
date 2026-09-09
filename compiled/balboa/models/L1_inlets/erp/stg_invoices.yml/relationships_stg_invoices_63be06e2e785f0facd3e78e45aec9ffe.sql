
    
    

with child as (
    select vendor_id as from_field
    from BALBOA.L1_ERP.stg_invoices
    where vendor_id is not null
),

parent as (
    select vendor_id as to_field
    from BALBOA.L1_ERP.stg_vendors
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


