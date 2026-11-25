-- Answers "how many active vendors do we have?" per the Atlas "Active Vendor" definition.

select
    activity_as_of_date,
    activity_window_start_date,
    count(distinct case when is_active_vendor then vendor_id end) as active_vendor_count,
    count(distinct case when is_dormant_vendor then vendor_id end) as dormant_vendor_count,
    count(distinct vendor_id) as vendor_count,
    count(distinct case when is_recorded_status_mismatch then vendor_id end) as recorded_status_mismatch_vendor_count
from {{ ref('dim_vendor') }}
group by activity_as_of_date, activity_window_start_date
