
    
    

with child as (
    select vendor_id as from_field
    from BALBOA.L2_INVOICES.fct_invoices
    where vendor_id is not null
),

parent as (
    select vendor_id as to_field
    from BALBOA.L2_VENDORS.dim_vendors
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


