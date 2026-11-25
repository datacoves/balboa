select
    po_id::varchar as po_id,
    vendor_id::varchar as vendor_id,
    po_date::date as po_date,
    po_amount::number(38, 0) as po_amount
from {{ ref('seed_purchase_orders') }}
