{{ config(tags=['requires_fixture_data']) }}

with reference_dates as (

    select
        to_date('2026-06-01') as reference_date,
        32 as expected_total,
        17 as expected_active
    union all
    select
        to_date('2026-07-01') as reference_date,
        33 as expected_total,
        24 as expected_active
    union all
    select
        to_date('2026-08-01') as reference_date,
        34 as expected_total,
        25 as expected_active

),

activity as (

    select
        vendor_id,
        po_date as activity_date
    from {{ ref('fct_purchase_orders') }}

    union all

    select
        vendor_id,
        invoice_date as activity_date
    from {{ ref('fct_invoices') }}

),

actual_counts as (

    select
        reference_dates.reference_date,
        count(distinct dim_vendors.vendor_id) as actual_total,
        count(distinct activity.vendor_id) as actual_active
    from reference_dates
    left join {{ ref('dim_vendors') }} as dim_vendors
        on dim_vendors.onboarded_date <= reference_dates.reference_date
    left join activity
        on dim_vendors.vendor_id = activity.vendor_id
            and activity.activity_date > dateadd(month, -12, reference_dates.reference_date)
            and activity.activity_date <= reference_dates.reference_date
    group by reference_dates.reference_date

)

select
    reference_dates.reference_date,
    reference_dates.expected_total,
    actual_counts.actual_total,
    reference_dates.expected_active,
    actual_counts.actual_active
from reference_dates
inner join actual_counts
    on reference_dates.reference_date = actual_counts.reference_date
where reference_dates.expected_total <> actual_counts.actual_total
    or reference_dates.expected_active <> actual_counts.actual_active
