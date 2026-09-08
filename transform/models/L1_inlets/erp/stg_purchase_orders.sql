with month_spine as (

    {{ procurement_month_spine() }}

),

vendors as (

    select
        vendor_id,
        category,
        onboarded_date,
        po_active_through_month,
        vendor_seq
    from {{ ref('stg_vendors') }}

)

select
    concat(vendors.vendor_id, '-PO-', to_char(month_spine.month_start, 'YYYYMM'))::varchar as po_id,
    vendors.vendor_id,
    month_spine.month_start::date as po_date,
    vendors.category,
    (
        case vendors.category
            when 'CRO Services' then 24000
            when 'Clinical Reagents' then 18000
            when 'IT / Software' then 12000
            when 'Logistics' then 9000
            when 'Lab Supplies' then 15000
            when 'Packaging' then 7000
            when 'Consulting' then 16000
            when 'Facilities' then 11000
        end
        + mod(vendors.vendor_seq, 4) * 125
    )::bigint as po_amount
from vendors
inner join month_spine
    on month_spine.month_start >= vendors.onboarded_date
        and month_spine.month_start <= vendors.po_active_through_month
where mod(datediff(month, vendors.onboarded_date, month_spine.month_start), 3) = 0
