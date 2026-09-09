



select
    *
from BALBOA.L2_PURCHASE_ORDERS.fct_purchase_orders

where not(po_amount > 0)

