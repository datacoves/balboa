
  create or replace   view BALBOA_STAGING.L1_ERP.stg_invoices__erp
  
    
    
(
  
    "INVOICE_ID" COMMENT $$Invoice primary key.$$, 
  
    "PO_ID" COMMENT $$Purchase order the invoice bills against.$$, 
  
    "VENDOR_ID" COMMENT $$Vendor that issued the invoice.$$, 
  
    "INVOICE_DATE" COMMENT $$Invoice date.$$, 
  
    "INVOICE_AMOUNT" COMMENT $$Invoice amount in USD.$$, 
  
    "PAID_DATE" COMMENT $$Date the invoice was paid; null while open.$$, 
  
    "INVOICE_STATUS" COMMENT $$Payment status of the invoice (open / paid).$$
  
)

  copy grants
  
  
  as (
    select
    invoice_id::varchar as invoice_id,
    po_id::varchar as po_id,
    vendor_id::varchar as vendor_id,
    invoice_date::date as invoice_date,
    invoice_amount::number(38, 0) as invoice_amount,
    paid_date::date as paid_date,
    invoice_status::varchar as invoice_status
from SEEDS.seed_invoices
  );

