select
    vendor_id::varchar as vendor_id,
    vendor_name::varchar as vendor_name,
    category::varchar as vendor_category,
    status::varchar as vendor_recorded_status,
    onboarded_date::date as vendor_onboarded_date
from {{ ref('seed_vendors') }}
