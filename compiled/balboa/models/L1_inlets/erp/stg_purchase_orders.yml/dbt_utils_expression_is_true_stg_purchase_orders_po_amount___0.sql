



select
    *
from BALBOA.L1_ERP.stg_purchase_orders

where not(po_amount > 0)

