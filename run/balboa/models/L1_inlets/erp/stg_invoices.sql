
  create or replace   view BALBOA_STAGING.L1_ERP.stg_invoices
  
    
    
(
  
    "INVOICE_ID" COMMENT $$Invoice primary key.$$, 
  
    "PO_ID" COMMENT $$Billed purchase order foreign key.$$, 
  
    "VENDOR_ID" COMMENT $$Vendor foreign key.$$, 
  
    "INVOICE_DATE" COMMENT $$Invoice activity date.$$, 
  
    "INVOICE_AMOUNT" COMMENT $$Positive whole-currency invoice amount equal to the PO amount.$$, 
  
    "PAID_DATE" COMMENT $$Payment date; null while the invoice remains open.$$, 
  
    "INVOICE_STATUS" COMMENT $$Payment state for the invoice.$$
  
)

  copy grants
  
  
  as (
    with purchase_orders as (

    select
        po_id,
        vendor_id,
        po_date,
        po_amount
    from L1_ERP.stg_purchase_orders

),

vendors as (

    select
        vendor_id,
        vendor_seq,
        activity_bucket,
        invoice_po_through_month,
        invoice_lag_months
    from L1_ERP.stg_vendors

),

candidate_invoices as (

    select
        purchase_orders.po_id,
        purchase_orders.vendor_id,
        purchase_orders.po_amount,
        vendors.vendor_seq,
        cast(dateadd(month, vendors.invoice_lag_months, purchase_orders.po_date) as date) as invoice_date
    from purchase_orders
    inner join vendors
        on purchase_orders.vendor_id = vendors.vendor_id
    where purchase_orders.po_date <= vendors.invoice_po_through_month
        and (
            vendors.activity_bucket <> 'C'
            or purchase_orders.po_date = vendors.invoice_po_through_month
        )

),

invoices_in_span as (

    select
        po_id,
        vendor_id,
        po_amount,
        vendor_seq,
        invoice_date
    from candidate_invoices
    where invoice_date <= 
    
    date_trunc('month', dateadd(month, -1, to_date('2026-09-08')))


)

select
    replace(po_id, '-PO-', '-INV-')::varchar as invoice_id,
    po_id,
    vendor_id,
    invoice_date,
    po_amount::bigint as invoice_amount,
    case
        when mod(vendor_seq + month(invoice_date), 5) = 0 then null
        else dateadd(day, 15, invoice_date)::date
    end as paid_date,
    case
        when mod(vendor_seq + month(invoice_date), 5) = 0 then 'open'
        else 'paid'
    end::varchar as invoice_status
from invoices_in_span
  );

