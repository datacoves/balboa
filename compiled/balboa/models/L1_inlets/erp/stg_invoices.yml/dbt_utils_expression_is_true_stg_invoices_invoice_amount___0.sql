



select
    *
from BALBOA.L1_ERP.stg_invoices

where not(invoice_amount > 0)

