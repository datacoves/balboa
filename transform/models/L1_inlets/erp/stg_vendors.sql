with month_spine as (

    {{ procurement_month_spine() }}

),

authored_vendors as (

    select
        vendor_id::varchar as vendor_id,
        vendor_name::varchar as vendor_name,
        category::varchar as category,
        status::varchar as status,
        onboarded_date::date as onboarded_date
    from {{ ref('seed_vendors') }}

),

archetypes as (

    select
        archetype_index::integer as archetype_index,
        vendor_name::varchar as vendor_name,
        category::varchar as category,
        status::varchar as status
    from {{ ref('seed_vendor_archetypes') }}

),

generated_months as (

    select
        month_start,
        row_number() over (order by month_start) as generated_vendor_number
    from month_spine
    where month_start > to_date('{{ var("authored_history_through") }}')

),

generated_vendors as (

    select
        concat('VENT', to_char(generated_months.month_start, 'YYYYMM'))::varchar as vendor_id,
        concat(archetypes.vendor_name, ' ', to_char(generated_months.month_start, 'YYYYMM'))::varchar as vendor_name,
        archetypes.category,
        archetypes.status,
        generated_months.month_start::date as onboarded_date
    from generated_months
    join archetypes
        on archetypes.archetype_index = mod(
                generated_months.generated_vendor_number - 1,
                (select count(*) from archetypes)
            )

),

all_vendors as (

    select * from authored_vendors
    union all
    select * from generated_vendors

),

bucketed as (

    select
        vendor_id,
        vendor_name,
        category,
        status,
        onboarded_date,
        row_number() over (order by vendor_id) as vendor_seq,
        case
            when vendor_id like 'VENT%' then 'A'
            when to_number(replace(vendor_id, 'VEN', '')) <= 6 then 'A'
            when to_number(replace(vendor_id, 'VEN', '')) <= 11 then 'B'
            when to_number(replace(vendor_id, 'VEN', '')) <= 17 then 'C'
            else 'D'
        end::varchar as activity_bucket
    from all_vendors

)

select
    vendor_id,
    vendor_name,
    category,
    status,
    onboarded_date,
    vendor_seq,
    activity_bucket,
    case
        when activity_bucket in ('A', 'B') then {{ trailing_edge_month() }}
        else dateadd(month, -15, {{ trailing_edge_month() }})
    end::date as po_active_through_month,
    case
        when activity_bucket = 'B' then dateadd(month, -15, {{ trailing_edge_month() }})
        when activity_bucket in ('A', 'B') then {{ trailing_edge_month() }}
        else dateadd(month, -15, {{ trailing_edge_month() }})
    end::date as invoice_po_through_month,
    case
        when activity_bucket = 'C' then 14
        else 1
    end::integer as invoice_lag_months
from bucketed
