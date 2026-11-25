
  create or replace   view BALBOA_STAGING.L1_ERP.stg_purchase_orders__erp
  
    
    
(
  
    "PO_ID" COMMENT $$Purchase order primary key.$$, 
  
    "VENDOR_ID" COMMENT $$Vendor the purchase order was issued to.$$, 
  
    "PO_DATE" COMMENT $$Date the purchase order was issued.$$, 
  
    "PO_AMOUNT" COMMENT $$Purchase order amount in USD.$$
  
)

  copy grants
  
  
  as (
    select
    po_id::varchar as po_id,
    vendor_id::varchar as vendor_id,
    po_date::date as po_date,
    po_amount::number(38, 0) as po_amount
from SEEDS.seed_purchase_orders
  );

