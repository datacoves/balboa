select
    vendor_id,
    vendor_name,
    category,
    status,
    onboarded_date
from {{ ref('stg_vendors') }}
