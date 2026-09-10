{{
    config(
        materialized='table'
    )
}}

with purchase_orders as (

    select
        vendor_id,
        po_date as activity_date
    from {{ ref('fct_purchase_orders') }}

),

invoices as (

    select
        vendor_id,
        invoice_date as activity_date
    from {{ ref('fct_invoices') }}

),

activity as (

    select * from purchase_orders
    union all
    select * from invoices

),

bounds as (

    select
        min(activity_date) as first_activity_date,
        max(activity_date) as latest_activity_date
    from activity

),

calendar_days as (

    select dateadd(day, seq4(), bounds.first_activity_date) as calendar_date
    from bounds,
        table(generator(rowcount => 10000))
    where dateadd(day, seq4(), bounds.first_activity_date) <= bounds.latest_activity_date

),

period_ends as (

    select distinct
        'week' as period_grain,
        dateadd(day, 7 - dayofweekiso(calendar_date), calendar_date) as period_end_date
    from calendar_days

    union all

    select distinct
        'month' as period_grain,
        last_day(calendar_date, 'month') as period_end_date
    from calendar_days

    union all

    select distinct
        'quarter' as period_grain,
        last_day(calendar_date, 'quarter') as period_end_date
    from calendar_days

),

completed_period_ends as (

    select
        period_ends.period_grain,
        period_ends.period_end_date
    from period_ends
    cross join bounds
    where period_ends.period_end_date <= bounds.latest_activity_date

),

active_vendor_periods as (

    select distinct
        completed_period_ends.period_grain,
        completed_period_ends.period_end_date,
        activity.vendor_id
    from completed_period_ends
    inner join activity
        on activity.activity_date > dateadd(day, -90, completed_period_ends.period_end_date)
        and activity.activity_date <= completed_period_ends.period_end_date

)

select
    active_vendor_periods.period_grain,
    active_vendor_periods.period_end_date,
    active_vendor_periods.vendor_id,
    vendors.vendor_name,
    vendors.category,
    vendors.status as recorded_status
from active_vendor_periods
inner join {{ ref('dim_vendors') }} as vendors
    on active_vendor_periods.vendor_id = vendors.vendor_id
