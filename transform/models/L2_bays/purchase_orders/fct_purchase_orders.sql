select
    po_id,
    vendor_id,
    po_date,
    category,
    po_amount
from {{ ref('stg_purchase_orders') }}
