
  create or replace   view BALBOA_STAGING.L2_PURCHASE_ORDERS.fct_purchase_orders
  
    
    
(
  
    "PO_ID" COMMENT $$Purchase order primary key.$$, 
  
    "VENDOR_ID" COMMENT $$Vendor foreign key.$$, 
  
    "PO_DATE" COMMENT $$First day of the purchase order month.$$, 
  
    "CATEGORY" COMMENT $$Vendor category carried onto the purchase order.$$, 
  
    "PO_AMOUNT" COMMENT $$Positive committed spend in whole currency units.$$
  
)

  copy grants
  
  
  as (
    select
    po_id,
    vendor_id,
    po_date,
    category,
    po_amount
from L1_ERP.stg_purchase_orders
  );

