select
    invoice_id,
    po_id,
    vendor_id,
    invoice_date,
    invoice_amount,
    paid_date,
    invoice_status
from {{ ref('stg_invoices') }}
