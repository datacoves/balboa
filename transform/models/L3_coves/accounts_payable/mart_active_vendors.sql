{{ config(materialized='table') }}

with purchase_orders as (

    select
        vendor_id,
        po_date as activity_date,
        'purchase_order' as activity_source
    from {{ ref('fct_purchase_orders') }}

),

invoices as (

    select
        vendor_id,
        invoice_date as activity_date,
        'invoice' as activity_source
    from {{ ref('fct_invoices') }}

),

activity as (

    select * from purchase_orders
    union all
    select * from invoices

),

bounds as (

    select max(activity_date) as reference_date
    from activity

)

select
    vendors.vendor_id,
    vendors.vendor_name,
    vendors.category,
    vendors.status as recorded_status,
    bounds.reference_date,
    max(case when activity.activity_source = 'purchase_order' then activity.activity_date end)
        as last_po_date,
    max(case when activity.activity_source = 'invoice' then activity.activity_date end)
        as last_invoice_date,
    count_if(
        activity.activity_source = 'purchase_order'
        and activity.activity_date > dateadd(month, -12, bounds.reference_date)
        and activity.activity_date <= bounds.reference_date
    ) as trailing_12_month_po_count,
    count_if(
        activity.activity_source = 'invoice'
        and activity.activity_date > dateadd(month, -12, bounds.reference_date)
        and activity.activity_date <= bounds.reference_date
    ) as trailing_12_month_invoice_count,
    (
        trailing_12_month_po_count + trailing_12_month_invoice_count > 0
    ) as is_active
from {{ ref('dim_vendors') }} as vendors
cross join bounds
left join activity
    on vendors.vendor_id = activity.vendor_id
group by
    vendors.vendor_id,
    vendors.vendor_name,
    vendors.category,
    vendors.status,
    bounds.reference_date
