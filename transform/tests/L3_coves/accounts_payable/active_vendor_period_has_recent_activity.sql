with activity as (

    select
        vendor_id,
        po_date as activity_date
    from {{ ref('fct_purchase_orders') }}

    union all

    select
        vendor_id,
        invoice_date as activity_date
    from {{ ref('fct_invoices') }}

)

select
    active_vendor_periods.period_grain,
    active_vendor_periods.period_end_date,
    active_vendor_periods.vendor_id
from {{ ref('mart_active_vendors_by_period') }} as active_vendor_periods
left join activity
    on active_vendor_periods.vendor_id = activity.vendor_id
    and activity.activity_date > dateadd(day, -90, active_vendor_periods.period_end_date)
    and activity.activity_date <= active_vendor_periods.period_end_date
group by
    active_vendor_periods.period_grain,
    active_vendor_periods.period_end_date,
    active_vendor_periods.vendor_id
having count(activity.activity_date) = 0
