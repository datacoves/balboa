
    
    

with child as (
    select po_id as from_field
    from BALBOA.L1_ERP.stg_invoices
    where po_id is not null
),

parent as (
    select po_id as to_field
    from BALBOA.L1_ERP.stg_purchase_orders
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


