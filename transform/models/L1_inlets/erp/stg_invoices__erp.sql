select
    invoice_id::varchar as invoice_id,
    po_id::varchar as po_id,
    vendor_id::varchar as vendor_id,
    invoice_date::date as invoice_date,
    invoice_amount::number(38, 0) as invoice_amount,
    paid_date::date as paid_date,
    invoice_status::varchar as invoice_status
from {{ ref('seed_invoices') }}
