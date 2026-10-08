
  create or replace   view BALBOA_STAGING.L2_INVOICES.fct_invoices
  
    
    
(
  
    "INVOICE_ID" COMMENT $$Invoice primary key.$$, 
  
    "PO_ID" COMMENT $$Billed purchase order foreign key.$$, 
  
    "VENDOR_ID" COMMENT $$Vendor foreign key.$$, 
  
    "INVOICE_DATE" COMMENT $$Invoice activity date.$$, 
  
    "INVOICE_AMOUNT" COMMENT $$Positive whole-currency invoice amount equal to the PO amount.$$, 
  
    "PAID_DATE" COMMENT $$Null while the invoice is open.$$, 
  
    "INVOICE_STATUS" COMMENT $$Payment state for the invoice.$$
  
)

  copy grants
  
  
  as (
    select
    invoice_id,
    po_id,
    vendor_id,
    invoice_date,
    invoice_amount,
    paid_date,
    invoice_status
from L1_ERP.stg_invoices
  );

