



select
    *
from BALBOA.L2_INVOICES.fct_invoices

where not(invoice_amount > 0)

