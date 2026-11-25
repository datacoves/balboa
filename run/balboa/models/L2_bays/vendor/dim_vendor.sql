
  create or replace   view BALBOA_STAGING.L2_VENDOR.dim_vendor
  
    
    
(
  
    "VENDOR_ID" COMMENT $$Vendor primary key; vendors are identified and counted by this id.$$, 
  
    "VENDOR_NAME" COMMENT $$Vendor display name.$$, 
  
    "VENDOR_CATEGORY" COMMENT $$Vendor spend category.$$, 
  
    "VENDOR_RECORDED_STATUS" COMMENT $$Status flag (active / inactive) as recorded in the ERP. Informational only.$$, 
  
    "VENDOR_ONBOARDED_DATE" COMMENT $$Date the vendor was onboarded. Not used for classification (no grace period).$$, 
  
    "ACTIVITY_AS_OF_DATE" COMMENT $$Latest purchase order, invoice or payment date in the procurement data.$$, 
  
    "ACTIVITY_WINDOW_START_DATE" COMMENT $$As-of date minus 12 months; this day is inside the window.$$, 
  
    "WINDOW_PO_COUNT" COMMENT $$Number of purchase orders dated within the window.$$, 
  
    "WINDOW_INVOICE_COUNT" COMMENT $$Number of invoices dated within the window. Payments are not counted.$$, 
  
    "WINDOW_PO_DATE_MAX" COMMENT $$Most recent purchase order date within the window; null if none.$$, 
  
    "WINDOW_INVOICE_DATE_MAX" COMMENT $$Most recent invoice date within the window; null if none.$$, 
  
    "IS_ACTIVE_VENDOR" COMMENT $$True if the vendor had a purchase order or invoice dated within the window (Atlas "Active Vendor").$$, 
  
    "IS_DORMANT_VENDOR" COMMENT $$Exact complement of is_active_vendor (Atlas "Dormant Vendor").$$, 
  
    "IS_RECORDED_STATUS_MISMATCH" COMMENT $$True when the Active / Dormant classification disagrees with the recorded ERP status flag.$$
  
)

  copy grants
  
  
  as (
    -- One row per vendor with its procurement activity and Active / Dormant classification.
-- Governed business logic: Atlas definitions of record "Vendor", "Dormant Vendor" and "Active Vendor".

with vendors as (
    select * from L1_ERP.stg_vendors__erp
),

purchase_orders as (
    select * from L1_ERP.stg_purchase_orders__erp
),

invoices as (
    select * from L1_ERP.stg_invoices__erp
),

-- As-of date: latest PO, invoice or payment date in the data (not today's date).
-- The window starts 12 months earlier and that day is inside the window.
as_of as (
    select
        max(activity_date) as activity_as_of_date,
        dateadd(month, -12, max(activity_date)) as activity_window_start_date
    from (
        select po_date as activity_date from purchase_orders
        union all
        select invoice_date from invoices
        union all
        select paid_date from invoices where paid_date is not null
    )
),

-- Qualifying activity: a PO or invoice dated in the window. Payments alone do not qualify.
po_activity as (
    select
        purchase_orders.vendor_id,
        count(*) as window_po_count,
        max(purchase_orders.po_date) as window_po_date_max
    from purchase_orders
    cross join as_of
    where purchase_orders.po_date between as_of.activity_window_start_date and as_of.activity_as_of_date
    group by purchase_orders.vendor_id
),

invoice_activity as (
    select
        invoices.vendor_id,
        count(*) as window_invoice_count,
        max(invoices.invoice_date) as window_invoice_date_max
    from invoices
    cross join as_of
    where invoices.invoice_date between as_of.activity_window_start_date and as_of.activity_as_of_date
    group by invoices.vendor_id
),

activity as (
    select
        vendors.vendor_id,
        vendors.vendor_name,
        vendors.vendor_category,
        vendors.vendor_recorded_status,
        vendors.vendor_onboarded_date,
        as_of.activity_as_of_date,
        as_of.activity_window_start_date,
        coalesce(po_activity.window_po_count, 0) as window_po_count,
        coalesce(invoice_activity.window_invoice_count, 0) as window_invoice_count,
        po_activity.window_po_date_max,
        invoice_activity.window_invoice_date_max
    from vendors
    cross join as_of
    left join po_activity
        on vendors.vendor_id = po_activity.vendor_id
    left join invoice_activity
        on vendors.vendor_id = invoice_activity.vendor_id
),

classified as (
    select
        *,
        (window_po_count > 0 or window_invoice_count > 0) as is_active_vendor
    from activity
)

select
    *,
    not is_active_vendor as is_dormant_vendor,
    is_active_vendor != (vendor_recorded_status = 'active') as is_recorded_status_mismatch
from classified
  );

